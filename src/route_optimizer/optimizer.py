import logging
import random
import json
import re
import requests
import urllib3
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql.functions import lit, col
from delta.tables import DeltaTable
from route_optimizer.awsmanager import AWSSessionManager
import time

class RouteOptimizer:
    def __init__(self, spark, aws_session: AWSSessionManager, delta_table_path, routify_token, bucket_name, folder_name, depots, fleet_params):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        self.spark = spark
        self.aws_session = aws_session
        self.s3_client = aws_session.s3_client
        self.delta_table_path = delta_table_path
        self.routify_token = routify_token
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.depots = depots
        self.fleet_for_routific = self.build_fleet(depots, **fleet_params)

    def read_ready_for_dispatch_orders(self):
        try:
            delta_table = DeltaTable.forPath(self.spark, self.delta_table_path)
            df = delta_table.toDF().filter("status = 'READY_FOR_DISPATCH'")
            return df.collect()
        except Exception as e:
            self.logger.error(f"Error reading Delta table: {e}")
            raise

    def format_to_visits(self, data):
        priorities = ["low", "regular", "high"]
        if data is None:
            self.logger.error("Data is None, cannot format visits.")
            return {}

        visits = {
            item['order_id']: {
                "location": {
                    "name": item['customer_id'],
                    "lat": round(item['lat'], 6),
                    "lng": round(item['lon'], 7)
                },
                "start": "9:00",
                "end": "18:00",
                "duration": 5,
                "load": {
                    "weight": int(np.ceil(item['total_weight'])),
                    "volume": int(np.ceil(item['total_volume']))
                },
                "priority": random.choice(priorities),
            } for item in data
        }
        return visits

    def build_fleet(self, depots, num_drivers, shift_start, shift_end, weight, volume, drivers_per_depot):
        fleet = {
            f"driver_{driver_counter}": {
                "start_location": {
                    "id": depot["id"],
                    "name": depot["name"],
                    "lat": depot["lat"],
                    "lng": depot["lng"]
                },
                "end_location": {
                    "id": depot["id"],
                    "name": depot["name"],
                    "lat": depot["lat"],
                    "lng": depot["lng"]
                },
                "shift_start": shift_start,
                "shift_end": shift_end,
                "min_visits": 1,
                "capacity": {
                    "weight": weight,
                    "volume": volume
                }
            }
            for depot in depots
            for driver_counter in range(1, num_drivers + 1)
            if driver_counter <= num_drivers and (driver_counter - 1) // drivers_per_depot < len(depots)
        }
        return fleet

    def create_payload(self, visits, fleet):
        payload = {
            "visits": visits,
            "fleet": fleet
        }
        config = {
            "options": {
                "traffic": "slow",
                "balance": True,
                "shortest_distance": True,
                "polylines": True
            }
        }
        return {**payload, **config}

    def send_to_routific(self, payload):
        routific_url = "https://api.routific.com/v1/vrp-long"
        headers = {
            "Authorization": f"bearer {self.routify_token}",
            "Content-Type": "application/json"
        }
        response = requests.post(routific_url, headers=headers, json=payload)
        if response.status_code == 200 or response.status_code == 202:
            self.logger.info("Orders submitted to Routific. Processing in progress.")
            response_data = response.json()
            jobID = response_data.get("job_id")
            self.logger.info(f"Job ID: {jobID}")
            return jobID
        else:
            self.logger.error(f"Failed to submit the orders. Status code: {response.status_code}")
            self.logger.error("Response:", response.text)
            return None

    def check_job_status(self, URL, jobID, headers, waiting_time):
        http = urllib3.PoolManager()
        URL = f"{URL}/{jobID}"
        job_status = None

        while job_status != 'finished':
            response = http.request('GET', URL, headers=headers)
            solution_data = json.loads(response.data.decode('utf-8'))
            job_status = solution_data.get('status')

            self.logger.info("Current job status:", job_status)

            if job_status in ['pending', 'processing']:
                time.sleep(waiting_time)
            elif job_status == 'finished':
                self.logger.info("Job finished.")
                return solution_data
            else:
                self.logger.error("Unexpected job status:", job_status)
                break

    def upload_to_s3(self, solution_data):
        # Extract the finishedProcessingAt timestamp
        finished_processing_at = solution_data['timing']['finishedProcessingAt']
        # Format the timestamp to make it file-name friendly
        formatted_timestamp = re.sub(r'[:\s]', '-', finished_processing_at)
        # Convert the solution data to a JSON string
        json_data = json.dumps(solution_data, indent=4)
        # Define the S3 file name
        s3_file_name = f'{self.folder_name}dispatch_{formatted_timestamp}.json'
        # Upload the JSON string directly to S3
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=s3_file_name,
            Body=json_data,
            ContentType='application/json'
        )
        self.logger.info(f"JSON data uploaded to s3://{self.bucket_name}/{s3_file_name}")

    def overwrite_delta_table(self, served_customers):
        delta_table = DeltaTable.forPath(self.spark, self.delta_table_path)
        df = delta_table.toDF()
        served_df = self.spark.createDataFrame(served_customers, ['served_customer_id', 'updated_finish_time'])
        served_df = served_df.withColumnRenamed('updated_finish_time', 'new_finish_time')
        updated_df = df.join(served_df, df.customer_id == served_df.served_customer_id, 'inner') \
            .withColumn('status', lit('DISPATCHED')) \
            .withColumn('order_timestamp', col('new_finish_time'))
        delta_table.alias('old') \
            .merge(updated_df.alias('new'), 'old.customer_id = new.served_customer_id') \
            .whenMatchedUpdate(set={
                "status": col('new.status'),
                "order_timestamp": col('new.new_finish_time')
            }) \
            .execute()
        df_updated = delta_table.toDF().drop('served_customer_id', 'new_finish_time')
        df_updated.filter(df_updated.status.isin('DISPATCHED', 'READY_FOR_DISPATCH')).show(truncate=False)

    def run_optimizer(self):
        try:
            ready_orders = self.read_ready_for_dispatch_orders()
            visits_for_routific = self.format_to_visits(ready_orders)
            payload = self.create_payload(visits_for_routific, self.fleet_for_routific)
            jobID = self.send_to_routific(payload)
            if jobID:
                headers = {
                    "Authorization": f"bearer {self.routify_token}",
                    "Content-Type": "application/json"
                }
                solution_data = self.check_job_status("https://api.routific.com/jobs", jobID, headers, waiting_time=10)
                self.upload_to_s3(solution_data)
                served_customers = [(stop['location_name'], stop['finish_time']) for driver, stops in solution_data['output']['solution'].items() for stop in stops if stop['location_name'].startswith('cus-')]
                for index, (cus_id, finish_time) in enumerate(served_customers):
                    try:
                        finish_time_obj = datetime.strptime(finish_time, '%H:%M')
                        updated_finish_time = datetime.now().replace(hour=finish_time_obj.hour, minute=finish_time_obj.minute) + timedelta(days=1)
                        served_customers[index] = (cus_id, updated_finish_time)
                    except ValueError as e:
                        self.logger.error(f"Error parsing finish_time '{finish_time}' for cus_id '{cus_id}': {e}")
                self.overwrite_delta_table(served_customers)
        except Exception as exc:
            self.logger.error(f"An error occurred in run_optimizer: {exc}")
            raise