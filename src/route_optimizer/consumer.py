import json
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from route_optimizer.awsmanager import AWSSessionManager

logger = logging.getLogger(__name__)

class StreamReader:
    def __init__(self, aws_session: AWSSessionManager, spark: SparkSession, stream_name: str, bronze_table_path: str, shard_iterator_type: str = 'LATEST', batch_size: int = 10000, schema: StructType = None, buffer_time: int = 60 * 15, max_iterations: int = None):
        """
        Initialize the StreamReader with necessary parameters.
        """
        self.stream_name = stream_name
        self.batch_size = batch_size
        self.shard_iterator_type = shard_iterator_type
        self.schema = schema
        self.bronze_table_path = bronze_table_path
        self.buffer_time = buffer_time  # Time to accumulate records (in seconds)
        self.max_iterations = max_iterations  # Max iterations to run the stream reading loop

        # Use AWS session manager and Spark session
        self.aws_session = aws_session
        self.kinesis_client = aws_session.kinesis_client
        self.spark = spark

        logger.info(f"Initialized StreamReader for stream: {self.stream_name}, writing to Delta path: {self.bronze_table_path}")
        logger.info(f"Batch size: {self.batch_size}, Buffer time: {self.buffer_time}s, Shard iterator type: {self.shard_iterator_type}, Max iterations: {self.max_iterations}")

    def refresh_session(self):
        """
        Refresh the AWS session to handle token expiration.
        """
        logger.info("Refreshing AWS session.")
        self.aws_session.refresh_session()
        self.kinesis_client = self.aws_session.kinesis_client
        logger.info("AWS session refreshed successfully.")

    def read_kinesis_records(self, duration: int = 60) -> None:
        """
        Read batches of Kinesis records and write them to the bronze Delta table for a specified duration or a set number of iterations.
        """
        try:
            logger.info(f"Describing Kinesis stream: {self.stream_name}")
            response = self.kinesis_client.describe_stream(StreamName=self.stream_name)
            shard_id = response['StreamDescription']['Shards'][0]['ShardId']

            shard_iterator = self.kinesis_client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=shard_id,
                #ShardId = 'shardId-000000000000',
                ShardIteratorType=self.shard_iterator_type
            )['ShardIterator']
            
            records_buffer = []
            buffer_start_time = time.time()
            iteration_count = 0

            # Run until max_iterations or until duration limit
            while True:
                if self.max_iterations and iteration_count >= self.max_iterations:
                    logger.info(f"Reached maximum iterations ({self.max_iterations}), stopping stream reading.")
                    break

                response = self.kinesis_client.get_records(ShardIterator=shard_iterator, Limit=self.batch_size)
                records = response['Records']

                for record in records:
                    order_data = record['Data']
                    order = json.loads(order_data)
                    records_buffer.append(order)

                shard_iterator = response['NextShardIterator']

                # Write to Delta if buffer time has passed or batch size is reached
                if time.time() - buffer_start_time >= self.buffer_time or len(records_buffer) >= self.batch_size:
                    if records_buffer:
                        logger.info(f"Writing {len(records_buffer)} records to Bronze Delta table...")
                        self.write_to_bronze(records_buffer)
                        records_buffer = []
                        buffer_start_time = time.time()

                # # Break after the specified duration
                # if time.time() - buffer_start_time >= duration:
                #     logger.info(f"Reached maximum duration ({duration}s), stopping stream reading.")
                    break

                iteration_count += 1

        except Exception as e:
            logger.error(f"Error reading from Kinesis: {e}")
            raise

    def write_to_bronze(self, records_buffer):
        """
        Write buffered records to the Bronze Delta table in batch mode.
        """
        try:
            df_bronze = self.spark.createDataFrame(records_buffer, schema=self.schema)

            # Write the DataFrame to the Bronze Delta table in append mode
            (df_bronze.write
             .format("delta")
             .mode("append")
             .option("path", self.bronze_table_path)
             .save())
            
            time.sleep(10)
            logger.info(f"Data successfully written to Bronze Delta table.")
        except Exception as e:
            logger.error(f"Error writing DataFrame to Bronze Delta table: {e}")
            raise
