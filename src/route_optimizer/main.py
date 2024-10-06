# import logging
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# from route_optimizer.awsmanager import AWSSessionManager
# from route_optimizer.consumer import StreamReader
# from route_optimizer.sparkmanager import SparkSessionManager

# # Set up the logger
# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO)

# def main():
#     # Initialize AWS Session Manager
#     aws_session = AWSSessionManager(env_file="path/to/.env")
#     logger.info("AWS Session Manager initialized.")

#     # Verify AWS Kinesis and S3 Clients
#     kinesis_client = aws_session.kinesis_client
#     s3_client = aws_session.s3_client
#     logger.info("AWS Kinesis and S3 clients initialized.")

#     # AWS credentials
#     aws_credentials = {
#         "aws_access_key_id": aws_session.aws_credentials["aws_access_key_id"],
#         "aws_secret_access_key": aws_session.aws_credentials["aws_secret_access_key"],
#         "aws_session_token": aws_session.aws_credentials["aws_session_token"],
#         "aws_region": aws_session.aws_credentials["aws_region"]
#     }

#     # Initialize Spark session
#     spark_manager = SparkSessionManager(aws_credentials=aws_credentials)
#     spark = spark_manager.spark
#     logger.info("Spark session initialized.")

#     # Define the schema for the DataFrame
#     schema = StructType([
#         StructField("order_id", StringType(), True),
#         StructField("customer_id", StringType(), True),
#         StructField("total_weight", DoubleType(), True),
#         StructField("total_volume", DoubleType(), True),
#         StructField("total_price", DoubleType(), True),
#         StructField("order_timestamp", StringType(), True),
#         StructField("status", StringType(), True),
#         StructField("lat", DoubleType(), True),
#         StructField("lon", DoubleType(), True)
#     ])

#     stream_name = "OrderStreamForDispatching"
#     delta_path = "s3a://orders-for-dispatch/bronze/"

#     # Initialize the StreamReader with the AWS session and Spark session
#     stream_reader = StreamReader(
#         aws_session=aws_session,
#         spark=spark,
#         stream_name=stream_name,
#         delta_path=delta_path,
#         shard_iterator_type='LATEST',  # Try changing this to 'TRIM_HORIZON' or 'AT_TIMESTAMP'
#         batch_size=10,
#         schema=schema
#     )

#     # Continuously read batches of records from Kinesis and create DataFrames
#     stream_reader.read_kinesis_records()

# if __name__ == "__main__":
#     main()

# import logging
# import os
# import time
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# from route_optimizer.awsmanager import AWSSessionManager
# from route_optimizer.consumer import StreamReader
# from route_optimizer.sparkmanager import SparkSessionManager
# from route_optimizer.dispatcher import Dispatcher
# from prefect import task, flow

# # Set up the logger
# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# # Set Prefect Cloud API key
# os.environ["PREFECT_API_KEY"] = "your-prefect-api-key"

# def load_config():
#     """
#     Load configuration from environment variables or a configuration file.
#     """
#     config = {
#         "env_file": os.getenv("ENV_FILE", "path/to/.env"),
#         "stream_name": os.getenv("STREAM_NAME", "OrderStreamForDispatching"),
#         "bronze_table_path": os.getenv("BRONZE_TABLE_PATH", "s3a://orders-for-dispatch/bronze"),
#         "silver_table_path": os.getenv("SILVER_TABLE_PATH", "s3a://orders-for-dispatch/silver"),
#         "weight_threshold": float(os.getenv("WEIGHT_THRESHOLD", 10)),
#         "volume_threshold": float(os.getenv("VOLUME_THRESHOLD", 500)),
#         "time_threshold": int(os.getenv("TIME_THRESHOLD", 60 * 45)),
#         "buffer_limit": int(os.getenv("BUFFER_LIMIT", 10000)),
#         "batch_size": int(os.getenv("BATCH_SIZE", 10)),
#         "shard_iterator_type": os.getenv("SHARD_ITERATOR_TYPE", 'TRIM_HORIZON'),
#         "stream_reader_duration": int(os.getenv("STREAM_READER_DURATION", 10))  # Duration in seconds
#     }
#     return config

# def initialize_aws_session(env_file):
#     """
#     Initialize AWS Session Manager.
#     """
#     aws_session = AWSSessionManager(env_file=env_file)
#     logger.info("AWS Session Manager initialized.")
#     return aws_session

# def initialize_spark_session(aws_credentials):
#     """
#     Initialize Spark session.
#     """
#     spark_manager = SparkSessionManager(aws_credentials=aws_credentials)
#     spark = spark_manager.spark
#     logger.info("Spark session initialized.")
#     return spark

# def define_schema():
#     """
#     Define the schema for the DataFrame.
#     """
#     schema = StructType([
#         StructField("order_id", StringType(), True),
#         StructField("customer_id", StringType(), True),
#         StructField("total_weight", DoubleType(), True),
#         StructField("total_volume", DoubleType(), True),
#         StructField("total_price", DoubleType(), True),
#         StructField("order_timestamp", StringType(), True),
#         StructField("status", StringType(), True),
#         StructField("lat", DoubleType(), True),
#         StructField("lon", DoubleType(), True)
#     ])
#     return schema

# @task
# def start_stream_reader(config, aws_session, spark, schema):
#     """
#     Start the StreamReader to read records from Kinesis and write to the bronze Delta table.
#     """
#     stream_reader = StreamReader(
#         aws_session=aws_session,
#         spark=spark,
#         stream_name=config["stream_name"],
#         delta_path=config["bronze_table_path"],
#         shard_iterator_type=config["shard_iterator_type"],
#         batch_size=config["batch_size"],
#         schema=schema
#     )
#     stream_reader.read_kinesis_records()

# @task
# def start_dispatcher(config, spark, schema):
#     """
#     Start the Dispatcher to read from the bronze Delta table, process records, and write to the silver Delta table.
#     """
#     dispatcher = Dispatcher(
#         spark=spark,
#         schema=schema,
#         delta_table_path=config["bronze_table_path"],
#         dispatch_table_path=config["silver_table_path"],
#         weight_threshold=config["weight_threshold"],
#         volume_threshold=config["volume_threshold"],
#         time_threshold=config["time_threshold"],
#         buffer_limit=config["buffer_limit"]
#     )
#     try:
#         dispatcher.read_and_dispatch_orders()
#     except Exception as exc:
#         logger.error(f"Error occurred during dispatching: {exc}")
#         raise

# @flow(name="Order Processing Flow")
# def order_processing_flow():
#     try:
#         # Load configuration
#         config = load_config()

#         # Initialize AWS Session Manager
#         aws_session = initialize_aws_session(config["env_file"])

#         # AWS credentials
#         aws_credentials = {
#             "aws_access_key_id": aws_session.aws_credentials["aws_access_key_id"],
#             "aws_secret_access_key": aws_session.aws_credentials["aws_secret_access_key"],
#             "aws_session_token": aws_session.aws_credentials["aws_session_token"],
#             "aws_region": aws_session.aws_credentials["aws_region"]
#         }

#         # Initialize Spark session
#         spark = initialize_spark_session(aws_credentials)

#         # Define the schema for the DataFrame
#         schema = define_schema()

#         # Start StreamReader task
#         start_stream_reader(config, aws_session, spark, schema)

#         # Introduce a delay to allow StreamReader to run for a specified duration
#         logger.info(f"StreamReader will run for {config['stream_reader_duration']} seconds.")
#         time.sleep(config['stream_reader_duration'])  # Sleep for the specified duration

#         # Start Dispatcher task
#         start_dispatcher(config, spark, schema)

#     except Exception as exc:
#         logger.error(f"An error occurred: {exc}")
#         raise

# if __name__ == "__main__":
#     order_processing_flow()

# import logging
# import os
# import time
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# from route_optimizer.awsmanager import AWSSessionManager
# from route_optimizer.consumer import StreamReader
# from route_optimizer.sparkmanager import SparkSessionManager
# from route_optimizer.dispatcher import Dispatcher
# from route_optimizer.optimizer import RouteOptimizer
# from prefect import task, flow

# # Set up the logger
# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# # Set Prefect Cloud API key
# os.environ["PREFECT_API_KEY"] = "your-prefect-api-key"

# def load_config():
#     config = {
#         "env_file": os.getenv("ENV_FILE", ".env"),
#         "stream_name": os.getenv("STREAM_NAME", "OrderStreamForDispatching"),
#         "bronze_table_path": os.getenv("BRONZE_TABLE_PATH", "s3a://dispatch/bronze"),
#         "silver_table_path": os.getenv("SILVER_TABLE_PATH", "s3a://dispatch/silver"),
#         "weight_threshold": float(os.getenv("WEIGHT_THRESHOLD", 10)),
#         "volume_threshold": float(os.getenv("VOLUME_THRESHOLD", 500)),
#         "time_threshold": int(os.getenv("TIME_THRESHOLD", 60 * 45)),
#         "buffer_limit": int(os.getenv("BUFFER_LIMIT", 10000)),
#         "batch_size": int(os.getenv("BATCH_SIZE", 10)),
#         "shard_iterator_type": os.getenv("SHARD_ITERATOR_TYPE", 'LATEST'),
#         "stream_reader_duration": int(os.getenv("STREAM_READER_DURATION", 10)),
#         "routify_token": os.getenv("ROUTIFY_TOKEN"),
#         "bucket_name": 'orders-for-dispatch',
#         "folder_name": 'gold-optimization/',
#         "depots": [
#             {"id": "depot_1", "name": "San Sebastian de los Reyes", "lat": 40.54510, "lng": -3.61184},
#             {"id": "depot_2", "name": "Alcorcón", "lat": 40.350370, "lng": -3.855863},
#             {"id": "depot_3", "name": "Vallecas", "lat": 40.36977, "lng": -3.59670}
#         ],
#         "fleet_params": {
#             "num_drivers": 18,
#             "shift_start": "9:00",
#             "shift_end": "18:00",
#             "weight": 1500,
#             "volume": 1500,
#             "drivers_per_depot": 6
#         }
#     }
#     return config

# def initialize_aws_session(env_file):
#     aws_session = AWSSessionManager(env_file=env_file)
#     logger.info("AWS Session Manager initialized.")
#     return aws_session

# def initialize_spark_session(aws_credentials):
#     spark_manager = SparkSessionManager(aws_credentials=aws_credentials)
#     spark = spark_manager.spark
#     logger.info("Spark session initialized.")
#     return spark

# def define_schema():
#     schema = StructType([
#         StructField("order_id", StringType(), True),
#         StructField("customer_id", StringType(), True),
#         StructField("total_weight", DoubleType(), True),
#         StructField("total_volume", DoubleType(), True),
#         StructField("total_price", DoubleType(), True),
#         StructField("order_timestamp", StringType(), True),
#         StructField("status", StringType(), True),
#         StructField("lat", DoubleType(), True),
#         StructField("lon", DoubleType(), True)
#     ])
#     return schema

# @task
# def start_stream_reader(config, aws_session, spark, schema):
# #     stream_reader = StreamReader(
# #         aws_session=aws_session,
# #         spark=spark,
# #         stream_name=config["stream_name"],
# #         delta_path=config["bronze_table_path"],
# #         shard_iterator_type=config["shard_iterator_type"],
# #         batch_size=config["batch_size"],
# #         schema=schema
# #     )
# #     stream_reader.read_kinesis_records(config["stream_reader_duration"])
#     pass

# @task
# def start_dispatcher(config, spark, schema):
#     dispatcher = Dispatcher(
#         spark=spark,
#         schema=schema,
#         bronze_table_path=config["bronze_table_path"],
#         silver_table_path=config["silver_table_path"],
#         weight_threshold=config["weight_threshold"],
#         volume_threshold=config["volume_threshold"],
#         time_threshold=config["time_threshold"],
#         buffer_limit=config["buffer_limit"]
#         )
#     try:
#         dispatcher.process_orders()
#     except Exception as exc:
#         logger.error(f"Error occurred during dispatching: {exc}")
#         raise
#     pass

# @task
# def start_optimizer(config, spark, aws_session):
#     # optimizer = RouteOptimizer(
#     #     spark=spark,
#     #     aws_session=aws_session,
#     #     delta_table_path=config["silver_table_path"],
#     #     routify_token=config["routify_token"],
#     #     bucket_name=config["bucket_name"],
#     #     folder_name=config["folder_name"],
#     #     depots=config["depots"],
#     #     fleet_params=config["fleet_params"]
#     # )
#     # optimizer.run_optimizer()
#     pass

# @flow(name="Order Processing Flow")
# def order_processing_flow():
#     try:
#         config = load_config()
#         aws_session = initialize_aws_session(config["env_file"])
#         aws_credentials = {
#             "aws_access_key_id": aws_session.aws_credentials["aws_access_key_id"],
#             "aws_secret_access_key": aws_session.aws_credentials["aws_secret_access_key"],
#             "aws_session_token": aws_session.aws_credentials["aws_session_token"],
#             "aws_region": aws_session.aws_credentials["aws_region"]
#         }
#         spark = initialize_spark_session(aws_credentials)
#         schema = define_schema()
#         start_stream_reader(config, aws_session, spark, schema)
#         logger.info(f"StreamReader will run for {config['stream_reader_duration']} seconds.")
#         #time.sleep(config['stream_reader_duration'])
#         start_dispatcher(config, spark, schema)
#         start_optimizer(config, spark, aws_session)
#     except Exception as exc:
#         logger.error(f"An error occurred: {exc}")
#         raise

# if __name__ == "__main__":
#     order_processing_flow()

import logging
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from route_optimizer.awsmanager import AWSSessionManager
from route_optimizer.sparkmanager import SparkSessionManager
from route_optimizer.dispatcher import Dispatcher
from route_optimizer.pipeline_analytics import PipelineAnalytics
from prefect import task, flow

# Set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Set Prefect Cloud API key
os.environ["PREFECT_API_KEY"] = "your-prefect-api-key"

def load_config():
    config = {
        "env_file": os.getenv("ENV_FILE", ".env"),
        "stream_name": os.getenv("STREAM_NAME", "OrderStreamForDispatching"),
        "bronze_table_path": os.getenv("BRONZE_TABLE_PATH", "s3a://order-for-dispatch/bronze"),
        "silver_table_path": os.getenv("SILVER_TABLE_PATH", "s3a://order-for-dispatch/silver"),
        "weight_threshold": float(os.getenv("WEIGHT_THRESHOLD", 10)),
        "volume_threshold": float(os.getenv("VOLUME_THRESHOLD", 500)),
        "time_threshold": int(os.getenv("TIME_THRESHOLD", 60 * 45)),
        "buffer_limit": int(os.getenv("BUFFER_LIMIT", 10000)),
        "batch_size": int(os.getenv("BATCH_SIZE", 10)),
        "shard_iterator_type": os.getenv("SHARD_ITERATOR_TYPE", 'LATEST'),
        "stream_reader_duration": int(os.getenv("STREAM_READER_DURATION", 10)),
        "routify_token": os.getenv("ROUTIFY_TOKEN"),
        "bucket_name": 'orders-for-dispatch',
        "folder_name": 'gold-optimization/',
        "depots": [
            {"id": "depot_1", "name": "San Sebastian de los Reyes", "lat": 40.54510, "lng": -3.61184},
            {"id": "depot_2", "name": "Alcorcón", "lat": 40.350370, "lng": -3.855863},
            {"id": "depot_3", "name": "Vallecas", "lat": 40.36977, "lng": -3.59670}
        ],
        "fleet_params": {
            "num_drivers": 18,
            "shift_start": "9:00",
            "shift_end": "18:00",
            "weight": 1500,
            "volume": 1500,
            "drivers_per_depot": 6
        }
    }
    return config

def initialize_aws_session(env_file):
    aws_session = AWSSessionManager(env_file=env_file)
    logger.info("AWS Session Manager initialized.")
    return aws_session

def initialize_spark_session(aws_credentials):
    spark_manager = SparkSessionManager(aws_credentials=aws_credentials)
    spark = spark_manager.spark
    logger.info("Spark session initialized.")
    return spark

def define_schema():
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("total_weight", DoubleType(), True),
        StructField("total_volume", DoubleType(), True),
        StructField("total_price", DoubleType(), True),
        StructField("order_timestamp", StringType(), True),
        StructField("status", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ])
    return schema

@task
def start_stream_reader(config, aws_session, spark, schema):
    stream_reader = StreamReader(
        aws_session=aws_session,
        spark=spark,
        stream_name=config["stream_name"],
        delta_path=config["bronze_table_path"],
        shard_iterator_type=config["shard_iterator_type"],
        batch_size=config["batch_size"],
        schema=schema
    )
    stream_reader.read_kinesis_records(config["stream_reader_duration"])
    pass

@task
def start_dispatcher(config, spark, schema):
    dispatcher = Dispatcher(
        spark=spark,
        schema=schema,
        bronze_table_path=config["bronze_table_path"],
        silver_table_path=config["silver_table_path"],
        weight_threshold=config["weight_threshold"],
        volume_threshold=config["volume_threshold"],
        time_threshold=config["time_threshold"],
        buffer_limit=config["buffer_limit"]
    )
    try:
        dispatcher.process_orders()
    except Exception as exc:
        logger.error(f"Error occurred during dispatching: {exc}")
        raise
    pass

@task
def start_optimizer(config, spark, aws_session):
    optimizer = RouteOptimizer(
        spark=spark,
        aws_session=aws_session,
        delta_table_path=config["silver_table_path"],
        routify_token=config["routify_token"],
        bucket_name=config["bucket_name"],
        folder_name=config["folder_name"],
        depots=config["depots"],
        fleet_params=config["fleet_params"]
    )
    optimizer.run_optimizer()
    pass

@task
def pipeline_analytics(config, spark):
    analytics = PipelineAnalytics(
        spark=spark,
        silver_table_path=config["silver_table_path"],
        routes_file_path="s3a://dispatched-orders/optimized-dispatched-gold/routes/",
        pBI_parquet_path="s3a://dispatched-orders/optimized-dispatched-gold/pBI/",
        delta_table_path="s3a://dispatched-orders/delta-table/"
    )
    try:
        analytics.run()
    except Exception as exc:
        logger.error(f"Error occurred during pipeline analytics: {exc}")
        raise

@flow(name="Order Processing Flow")
def order_processing_flow():
    try:
        config = load_config()
        aws_session = initialize_aws_session(config["env_file"])
        aws_credentials = {
            "aws_access_key_id": aws_session.aws_credentials["aws_access_key_id"],
            "aws_secret_access_key": aws_session.aws_credentials["aws_secret_access_key"],
            "aws_session_token": aws_session.aws_credentials["aws_session_token"],
            "aws_region": aws_session.aws_credentials["aws_region"]
        }
        spark = initialize_spark_session(aws_credentials)
        schema = define_schema()

        start_stream_reader(config, aws_session, spark, schema)
        logger.info(f"StreamReader will run for {config['stream_reader_duration']} seconds.")
        # time.sleep(config['stream_reader_duration'])
        start_dispatcher(config, spark, schema)
        start_optimizer(config, spark, aws_session)
        pipeline_analytics(config, spark)
    except Exception as exc:
        logger.error(f"An error occurred: {exc}")
        raise

if __name__ == "__main__":
    order_processing_flow()