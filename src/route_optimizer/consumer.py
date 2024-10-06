# import json
# import logging
# from route_optimizer.awsmanager import AWSSessionManager
# import time

# logger = logging.getLogger(__name__)

# class StreamReader:
#     def __init__(self, aws_session: AWSSessionManager, stream_name: str, shard_iterator_type: str = 'LATEST', batch_size: int = 100):
#         """
#         Initialize the StreamReader with necessary parameters.
#         """
#         self.stream_name = stream_name
#         self.batch_size = batch_size
#         self.shard_iterator_type = shard_iterator_type

#         # Use the provided AWS session manager
#         self.aws_session = aws_session
#         self.kinesis_client = aws_session.kinesis_client

#     def refresh_session(self):
#         """
#         Refresh the AWS session to handle token expiration.
#         """
#         self.aws_session.refresh_session()
#         self.kinesis_client = self.aws_session.kinesis_client

#     def read_kinesis_records(self) -> None:
#         """
#         Continuously read batches of Kinesis records and process them.
#         """
#         try:
#             # Refresh the session to handle token expiration
#             self.refresh_session()
#             logger.info("AWS session refreshed successfully.")

#             # Get the shard ID
#             response = self.kinesis_client.describe_stream(StreamName=self.stream_name)
#             shard_id = response['StreamDescription']['Shards'][0]['ShardId']
#             logger.info(f"Shard ID for stream '{self.stream_name}' retrieved: {shard_id}")

#             # Get the initial shard iterator
#             shard_iterator = self.kinesis_client.get_shard_iterator(
#                 StreamName=self.stream_name,
#                 ShardId=shard_id,
#                 ShardIteratorType=self.shard_iterator_type
#             )['ShardIterator']
#             logger.info(f"Shard iterator for stream '{self.stream_name}' retrieved.")

#             # Start a loop to continuously read records
#             while True:
#                 # Fetch records from Kinesis
#                 response = self.kinesis_client.get_records(ShardIterator=shard_iterator, Limit=self.batch_size)
#                 records = response['Records']
#                 logger.info(f"Fetched {len(records)} records from Kinesis stream '{self.stream_name}'.")

#                 for record in records:
#                     # Process the order data
#                     order_data = record['Data']
#                     order = json.loads(order_data)
#                     logger.info(f"Received order: {order}")

#                 # Update the shard iterator for the next batch of records
#                 shard_iterator = response['NextShardIterator']

#                 # Sleep to avoid hitting API rate limits
#                 time.sleep(1)

#         except self.kinesis_client.exceptions.ResourceNotFoundException:
#             logger.error(f"Stream {self.stream_name} not found.")
#         except self.kinesis_client.exceptions.ProvisionedThroughputExceededException:
#             logger.error("Throughput limit exceeded, please try again later.")
#         except self.kinesis_client.exceptions.InvalidArgumentException as e:
#             logger.error(f"Invalid argument: {e}")
#         except Exception as e:
#             logger.error(f"An error occurred while fetching data from Kinesis: {e}")
#             raise

import json
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from delta.tables import DeltaTable
from route_optimizer.awsmanager import AWSSessionManager

logger = logging.getLogger(__name__)

class StreamReader:
    def __init__(self, aws_session: AWSSessionManager, spark: SparkSession, stream_name: str, delta_path: str, shard_iterator_type: str = 'LATEST', batch_size: int = 100, schema: StructType = None):
        """
        Initialize the StreamReader with necessary parameters.
        """
        self.stream_name = stream_name
        self.batch_size = batch_size
        self.shard_iterator_type = shard_iterator_type
        self.schema = schema
        self.delta_path = delta_path

        # Use the provided AWS session manager and Spark session
        self.aws_session = aws_session
        self.kinesis_client = aws_session.kinesis_client
        self.spark = spark

    def refresh_session(self):
        """
        Refresh the AWS session to handle token expiration.
        """
        self.aws_session.refresh_session()
        self.kinesis_client = self.aws_session.kinesis_client

    def read_kinesis_records(self, duration: int = 60) -> None:
        """
        Continuously read batches of Kinesis records and process them.
        """
        try:
            # Refresh the session to handle token expiration
            self.refresh_session()
            logger.info("AWS session refreshed successfully.")

            # Get the shard ID
            response = self.kinesis_client.describe_stream(StreamName=self.stream_name)
            #shard_id = 'shardId-000000000000'
            shard_id = response['StreamDescription']['Shards'][0]['ShardId']
            logger.info(f"Shard ID for stream '{self.stream_name}' retrieved: {shard_id}")

            # Get the initial shard iterator
            shard_iterator = self.kinesis_client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=shard_id,
                ShardIteratorType=self.shard_iterator_type
            )['ShardIterator']
            logger.info(f"Initial shard iterator for stream '{self.stream_name}' retrieved.")

            while True:
                # Fetch records from Kinesis
                response = self.kinesis_client.get_records(ShardIterator=shard_iterator, Limit=self.batch_size)
                records = response['Records']
                logger.info(f"Fetched {len(records)} records from Kinesis stream '{self.stream_name}'.")

                if not records:
                    logger.warning("No records found in the stream. The stream might be empty or the shard iterator might be pointing to an empty location.")

                records_data = []
                for record in records:
                    # Process the order data
                    order_data = record['Data']
                    order = json.loads(order_data)
                    logger.info(f"Received order: {order}")
                    records_data.append(order)

                # Create a Spark DataFrame from the fetched records
                if records_data:
                    df = self.spark.createDataFrame(records_data, schema=self.schema)
                    logger.info("Created Spark DataFrame from fetched records.")
                    self.write_to_delta(df, self.delta_path)
                    # Clean up memory
                    df.unpersist()
                    logger.info("DataFrame unpersisted from memory.")
                else:
                    logger.warning("No records to convert to DataFrame.")

                # Update the shard iterator for the next batch of records
                shard_iterator = response['NextShardIterator']
                logger.info(f"Shard iterator updated for the next batch of records: {shard_iterator}")

                # Sleep to avoid hitting API rate limits
                time.sleep(3)

        except self.kinesis_client.exceptions.ResourceNotFoundException:
            logger.error(f"Stream {self.stream_name} not found.")
        except self.kinesis_client.exceptions.ProvisionedThroughputExceededException:
            logger.error("Throughput limit exceeded, please try again later.")
        except self.kinesis_client.exceptions.InvalidArgumentException as e:
            logger.error(f"Invalid argument: {e}")
        except Exception as e:
            logger.error(f"An error occurred while fetching data from Kinesis: {e}")
            raise

    def write_to_delta(self, df, path):
        """
        Write the DataFrame to S3 in Delta format.
        """
        try:
           (df
            .write
            .format("delta")
            .mode("append")
            .save(path)
           )
           logger.info(f"DataFrame written to Delta table at {path}")
        except Exception as e:
            logger.error(f"Error writing DataFrame to Delta table: {e}")
            raise