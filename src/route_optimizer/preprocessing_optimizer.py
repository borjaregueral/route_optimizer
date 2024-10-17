import os
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from datetime import datetime
import logging
from route_optimizer.config import load_config
from route_optimizer.awsmanager import AWSSessionManager

# Initialize logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Load configuration
config = load_config()

class EtlOptimizer:
    def __init__(self, config, spark, aws_session: AWSSessionManager):
        self.config = config
        self.spark = spark
        self.aws_session = aws_session
        self.s3_client = aws_session.s3_client
        self.silver_table_path = config["silver_optimizer_table_path"]
        self.to_optimizer_path = config["to_optimizer_path"]
        self.routific_token = config["routific_token"]
        self.routific_url = config["routific_url"]
        self.fleet_params = config["fleet_params"]
        self.depots = config["depots"]
        self.visit_start_time = config["visit_start_time"]
        self.visit_end_time = config["visit_end_time"]
        self.visit_duration = config["visit_duration"]
        self.bucket_name = config["optimization_bucket"]
        self.batching_name = config["batching"]

    def read_ready_for_dispatch_orders(self):
        try:
            silver_df = self.spark.read.format("delta").load(self.silver_table_path)
            ready_orders = silver_df.filter(col("status") == "READY_FOR_DISPATCH")
            logger.info(f"Loaded {ready_orders.count()} ready-for-dispatch orders from the Silver table.")
            return ready_orders
        except Exception as e:
            logger.error(f"Error reading Silver Delta table: {str(e)}")
            raise e

    def build_fleet(self):
        try:
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
                    "shift_start": self.fleet_params["shift_start"],
                    "shift_end": self.fleet_params["shift_end"],
                    "min_visits": self.fleet_params.get("min_visits", 1),
                    "capacity": {
                        "weight": self.fleet_params["weight"],
                        "volume": self.fleet_params["volume"]
                    }
                }
                for depot in self.depots
                for driver_counter in range(1, self.fleet_params["num_drivers"] + 1)
                if driver_counter <= self.fleet_params["num_drivers"] and (driver_counter - 1) // self.fleet_params["drivers_per_depot"] < len(self.depots)
            }
            logger.info(f"Fleet built with {len(fleet)} drivers.")
            return fleet
        except Exception as e:
            logger.error(f"Error building fleet: {str(e)}")
            raise e

    def build_visits(self, ready_orders):
        try:
            visits = {}
            for row in ready_orders.collect():
                visit_id = row['order_id']
                customer_id = row['customer_id']
                visits[visit_id] = {
                    "location": {
                        "name": customer_id,
                        "lat": row["lat"],
                        "lng": row["lon"]
                    },
                    "start": self.visit_start_time,
                    "end": self.visit_end_time,
                    "duration": self.visit_duration
                }
            logger.info(f"Built visits for {len(visits)} orders.")
            return visits
        except Exception as e:
            logger.error(f"Error building visits: {str(e)}")
            raise e

    def optimize_routes(self, visits, fleet):
        payload = {
            "visits": visits,
            "fleet": fleet,
            "options": self.config["options"]
        }

        headers = {
            "Authorization": f"Bearer {self.routific_token}",
            "Content-Type": "application/json"
        }

        try:
            response = requests.post(self.routific_url, headers=headers, json=payload)

            if response.status_code in [200, 202]:
                response_data = response.json()
                job_id = response_data.get("job_id")
                logger.info(f"Orders submitted to Routific. Job ID: {job_id}")
                return job_id, payload
            else:
                logger.error(f"Failed to submit the orders. Status code: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return None, None
        except Exception as e:
            logger.error(f"Error optimizing routes: {str(e)}")
            raise e

    def upload_solution_to_s3(self, solution_data, batch_id, job_id):
        try:
            # Generate current timestamp in the format YYYY-MM-DD-HH-MM-SS
            timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

            # Use to_optimizer_path and job_id for the folder structure
            folder_path = f"{self.to_optimizer_path}/{self.batching_name}/{job_id}"
            file_name = f"batch_{batch_id}_{timestamp}.json"
            full_path = f"{folder_path}/{file_name}"

            # Convert the solution data to JSON and upload to S3
            json_data = json.dumps(solution_data, indent=4)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=full_path,
                Body=json_data
            )
            logger.info(f"Uploaded batch {batch_id} to S3 under {full_path} successfully.")
        except Exception as e:
            logger.error(f"Error uploading batch {batch_id} to S3: {str(e)}")
            raise e

    def process_batches_and_optimize(self):
        try:
            # Read and filter orders that are ready for dispatch
            ready_orders = self.read_ready_for_dispatch_orders()

            # Group orders by batch_id
            batches_df = ready_orders.groupBy("batch_id").agg(
                F.collect_list(F.struct("order_id", "customer_id", "lat", "lon")).alias("orders")
            )

            for row in batches_df.collect():
                batch_id = row["batch_id"]
                orders = row["orders"]

                # Build visits for the batch
                visits = {order["order_id"]: {
                    "location": {
                        "name": order["customer_id"],
                        "lat": order["lat"],
                        "lng": order["lon"]
                    },
                    "start": self.visit_start_time,
                    "end": self.visit_end_time,
                    "duration": self.visit_duration
                } for order in orders}

                # Build the fleet for optimization
                fleet = self.build_fleet()

                # Optimize routes and get job_id and payload
                job_id, payload = self.optimize_routes(visits, fleet)

                if job_id:
                    # Upload the final payload to S3 under folder with job_id
                    self.upload_solution_to_s3(payload, batch_id, job_id)
                    logger.info(f"Batch {batch_id} successfully optimized with job ID {job_id}.")
                else:
                    logger.error(f"Failed to optimize batch {batch_id}.")
        except Exception as e:
            logger.error(f"Error processing batches: {str(e)}")
            raise e
