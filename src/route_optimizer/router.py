import logging
from tenacity import retry, wait_fixed, stop_after_delay, RetryError
import urllib3
import json
import time
from route_optimizer.awsmanager import AWSSessionManager
from datetime import datetime

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchOptimizer:
    def __init__(self, config, aws_session: AWSSessionManager):
        self.config = config
        self.aws_session = aws_session
        self.s3_client = aws_session.s3_client
        self.dispatching_folder = config["dispatching_folder"]
        self.routific_jobs = config["routific_jobs"]
        self.routific_token = config["routific_token"]
        self.optimized_folder = config["optimized_path"]  # Gold layer optimized folder
        self.bucket_name = config["optimization_bucket"]


    def list_dispatching_jobs(self):
        """
        List all job folders in the dispatching folder from S3.
        Each folder corresponds to a job_id that will be processed.
        """
        try:
            logger.info("Listing jobs in dispatching folder...")
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.dispatching_folder)
            jobs = set()
            if 'Contents' in response:
                for obj in response['Contents']:
                    folder = obj['Key'].split("/")[2]  # Assuming the structure: dispatching/job_id/...
                    jobs.add(folder)
            logger.info(f"Found {len(jobs)} jobs in dispatching folder.")
            return list(jobs)
        except Exception as e:
            logger.error(f"Error listing jobs in dispatching folder: {str(e)}")
            raise e

    #@retry(wait=wait_fixed(60), stop=stop_after_delay(30*60*60))  # Retry every minute, stop after 1 hour
    def check_job_status(self, job_id):
        """
        Poll Routific to check the status of a job every minute until it is finished.
        If the job status is finished, it returns the solution data.
        If it fails to finish within the allowed retries, it raises an error.
        """
        http = urllib3.PoolManager()
        url = f"{self.routific_jobs}/{job_id}"
        headers = {"Authorization": f"Bearer {self.routific_token}",
                   "Content-Type": "application/json"}

        logger.info(f"Checking job status for job_id: {job_id}")
        response = http.request('GET', url, headers=headers)
        solution_data = json.loads(response.data.decode('utf-8'))
        job_status = solution_data.get('status')

        logger.info(f"Current job {job_id} status: {job_status}")

        if job_status == 'finished':
            logger.info(f"Job {job_id} finished.")
            return solution_data  # Job finished, return the result
        elif job_status in ['pending', 'processing']:
            logger.info(f"Job {job_id} is still {job_status}, retrying...")
            raise Exception(f"Job {job_id} is still {job_status}, retrying...")  # Retry until finished
        else:
            logger.error(f"Unexpected job status for job {job_id}: {job_status}")
            raise Exception(f"Unexpected job status: {job_status}")  # Unexpected status, raise error

    def retrieve_job_results(self, job_id):
        """
        Retrieve the results of an optimized job from Routific once the job is finished.
        """
        url = f"{self.routific_jobs}/{job_id}"
        headers = {"Authorization": f"Bearer {self.routific_token}",
                   "Content-Type": "application/json"}

        logger.info(f"Retrieving job results for job_id: {job_id}")
        try:
            response = urllib3.PoolManager().request('GET', url, headers=headers)
            if response.status == 200:
                logger.info(f"Successfully retrieved the solution for job {job_id}.")
                return json.loads(response.data.decode('utf-8'))
            else:
                logger.error(f"Failed to retrieve solution for job {job_id}. Status code: {response.status}")
                return None
        except Exception as e:
            logger.error(f"Error retrieving job results for {job_id}: {str(e)}")
            raise e

    def store_results_in_gold(self, job_id, result):
        """
        Store the results of the optimization in the 'optimized' folder in the gold layer.
        """
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            file_name = f"{job_id}_optimized_{timestamp}.json"
            full_path = f"{self.optimized_folder}/{file_name}"

            logger.info(f"Storing results for job {job_id} in {full_path}...")
            # Upload the result as a JSON file
            json_data = json.dumps(result, indent=4)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=full_path,
                Body=json_data
            )
            logger.info(f"Stored results for job {job_id} in {full_path}.")
        except Exception as e:
            logger.error(f"Error storing results for job {job_id} in gold layer: {str(e)}")
            raise e

    def move_processed_job(self, job_id):
        """
        Move the processed job to a 'processed' folder in S3 under the silver directory after it has been successfully processed.
        """
        try:
            source_prefix = f"{self.dispatching_folder}/{job_id}"
            destination_prefix = f"silver/processed/{job_id}"  # Moving to 'silver/processed' folder

            # List all objects under the job_id folder
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=source_prefix)
            if 'Contents' in response:
                logger.info(f"Moving processed job {job_id} to 'silver/processed' folder.")
                for obj in response['Contents']:
                    copy_source = {'Bucket': self.bucket_name, 'Key': obj['Key']}
                    destination_key = obj['Key'].replace(self.dispatching_folder, 'silver/processed')
                    # Copy the object to the new location
                    self.s3_client.copy_object(CopySource=copy_source, Bucket=self.bucket_name, Key=destination_key)
                    # Delete the original object after copying
                    self.s3_client.delete_object(Bucket=self.bucket_name, Key=obj['Key'])
                logger.info(f"Job {job_id} moved to 'silver/processed' folder.")
        except Exception as e:
            logger.error(f"Error moving job {job_id}: {str(e)}")
            raise e


    def process_jobs(self):
        """
        Process all the jobs in the dispatching folder by sending them to Routific, waiting for completion,
        and storing the results in the gold layer. Includes a 5-minute wait between processing batches,
        but skips the wait if no jobs are found in the folder.
        """
        jobs = self.list_dispatching_jobs()

        if not jobs:
            logger.info("No jobs found to process. Exiting.")
            return  # Stop the process if no jobs are found.

        for idx, job_id in enumerate(jobs):
            logger.info(f"Processing job {job_id}...")
            try:
                # Poll Routific for the job status every minute until it finishes
                solution_data = self.check_job_status(job_id)

                # Retrieve and store the results
                if solution_data:
                    result = self.retrieve_job_results(job_id)
                    if result:
                        self.store_results_in_gold(job_id, result)
                        self.move_processed_job(job_id)  # Move the job after it is processed

                # Check if there are more jobs to process after this job
                if idx < len(jobs) - 1:  # If there are more jobs left, wait before processing the next one
                    logger.info("Waiting for 45 seconds before processing the next batch...")
                    time.sleep(45)  # Adjust the wait time here

            except RetryError:
                logger.error(f"Job {job_id} did not finish within the retry limit. Skipping this job.")
            except Exception as e:
                logger.error(f"Error processing job {job_id}: {str(e)}")

        # After processing all jobs, stop the process and indicate completion.
        logger.info("All jobs processed. Stopping the process.")
