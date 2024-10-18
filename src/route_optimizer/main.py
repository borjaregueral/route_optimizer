import logging
from route_optimizer.awsmanager import AWSSessionManager
from route_optimizer.sparkmanager import SparkSessionManager
from route_optimizer.consumer import StreamReader
from route_optimizer.dispatcher import Dispatcher
from route_optimizer.config import load_config
from route_optimizer.schema import define_bronze_schema
from route_optimizer.etl_optimizer import BronzeOptimizerAccumulator
from route_optimizer.preprocessing_optimizer import EtlOptimizer
from route_optimizer.router import BatchOptimizer
from route_optimizer.process_optimizer_solution import DeltaTableBuilder
from route_optimizer.routes import RoutesBuilder
from route_optimizer.pbi_data import PBITableProcessor
from route_optimizer.pbi_analytics import PBIAthenaTableManager
from prefect import task, flow

# Set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s')


# -------------------------------------------
# Tasks
# -------------------------------------------

@task(persist_result=False)
def initialize_aws_session(env_file):
    try:
        aws_session = AWSSessionManager(env_file=env_file)
        logger.info("AWS Session Manager initialized.")
        return aws_session
    except Exception as e:
        logger.error(f"Error initializing AWS session: {e}")
        raise


@task(persist_result=False)
def initialize_spark_session(aws_credentials):
    try:
        spark_manager = SparkSessionManager(aws_credentials=aws_credentials)
        spark = spark_manager.spark
        logger.info("Spark session initialized.")
        return spark
    except Exception as e:
        logger.error(f"Error initializing Spark session: {e}")
        raise


@task()
def start_stream_reader(config, aws_session, spark, schema):
    try:
        stream_reader = StreamReader(
            aws_session=aws_session,
            spark=spark,
            stream_name=config["stream_name"],
            bronze_table_path=config["bronze_table_path"],
            shard_iterator_type=config["shard_iterator_type"],
            batch_size=config["batch_size"],
            buffer_time=config["buffer_time"],
            schema=schema
        )
        stream_reader.read_kinesis_records(duration=config["stream_reader_duration"])
    except Exception as e:
        logger.error(f"Error in StreamReader: {e}")
        raise


@task()
def process_bronze_to_silver(config, spark):
    try:
        dispatcher = Dispatcher(spark=spark, config=config)
        dispatcher.process_orders_to_silver()
    except Exception as e:
        logger.error(f"Error in processing from Bronze to Silver: {e}")
        raise


@task()
def process_silver_to_gold(config, spark):
    try:
        dispatcher = Dispatcher(spark=spark, config=config)
        dispatcher.process_orders_to_gold()
    except Exception as e:
        logger.error(f"Error in processing from Silver to Gold: {e}")
        raise


@task()
def dispatcher_to_optimizer(config, spark):
    try:
        accumulator = BronzeOptimizerAccumulator(spark, config)
        accumulator.orders_from_dispatcher_to_optimizer_bronze()
    except Exception as e:
        logger.error(f"Error in processing from Gold to Bronze Optimizer: {e}")
        raise


@task()
def process_orders_in_optimizer(config, spark):
    try:
        accumulator = BronzeOptimizerAccumulator(spark, config)
        accumulator.process_orders_optimizer_from_bronze_to_silver()
    except Exception as e:
        logger.error(f"Error in processing orders in the Optimizer: {e}")
        raise


@task()
def etl_optimizer(config, spark, aws_session):
    try:
        optimizer = EtlOptimizer(spark=spark, config=config, aws_session=aws_session)
        optimizer.process_batches_and_optimize()
    except Exception as e:
        logger.error(f"Error during route optimization: {e}")
        raise


@task()
def optimize_routes(config, aws_session):
    try:
        optimizer = BatchOptimizer(config, aws_session)
        optimizer.process_jobs()
    except Exception as e:
        logger.error(f"Error optimizing routes: {e}")
        raise


@task()
def postprocessing(aws_session, spark, config):
    try:
        analytics_processor = DeltaTableBuilder(spark=spark, config=config, aws_session=aws_session)
        analytics_processor.process_and_save()
    except Exception as e:
        logger.error(f"Error during postprocessing: {e}")
        raise


@task()
def process_routes(config, aws_session, spark):
    try:
        routes_builder = RoutesBuilder(
            spark=spark,
            config=config,
            aws_session=aws_session
        )
        routes_builder.process_and_save()
    except Exception as e:
        logger.error(f"Error processing routes: {e}")
        raise


@task()
def pBI_processor(config, spark):
    try:
        pBI_builder = PBITableProcessor(
            spark=spark,
            config=config,
        )
        pBI_builder.process_and_save()
    except Exception as e:
        logger.error(f"Error processing PBI data: {e}")
        raise


@task(result_serializer="json", persist_result=False)
def pBI_tables(config, aws_session, spark):
    try:

        pBI_analytics_manager = PBIAthenaTableManager(
            aws_session=aws_session,
            config=config,
            spark = spark

        )
        pBI_analytics_manager.process_pbi_data()
        logger.info("PBI data processed successfully.")
    except Exception as e:
        logger.error(f"Error processing PBI data: {e}")
        raise


# -------------------------------------------
# Flow
# -------------------------------------------

@flow(name="Order Processing Flow")
def order_processing_flow():
    try:
        # Load configuration
        config = load_config()

        # Initialize AWS session
        aws_session = initialize_aws_session(config["env_file"])

        # Initialize Spark session
        spark = initialize_spark_session(aws_session.aws_credentials)

        # Define schema for Bronze table
        bronze_schema = define_bronze_schema()

        # # Start stream reader task
        # start_stream_reader(config, aws_session, spark, bronze_schema)

        # # Process Bronze to Silver
        # process_bronze_to_silver(config, spark)

        # # Process Silver to Gold
        # process_silver_to_gold(config, spark)

        # # Process Gold to Bronze optimizer
        # dispatcher_to_optimizer(config, spark)

        # # Process Bronze to Silver optimizer
        # process_orders_in_optimizer(config, spark)

        # # Route optimization
        # etl_optimizer(config, spark, aws_session)

        # # Optimize routes
        # optimize_routes(config, aws_session)

        # # Post-processing task for analytics
        # postprocessing(aws_session, spark, config)

        # # Process routes with RoutesBuilder
        # process_routes(config, aws_session, spark)

        # # # Process PBI data
        # pBI_processor(config, spark)

        # Create and update Athena tables for PBI
        pBI_tables(config, aws_session, spark)

    except Exception as e:
        logger.error(f"An error occurred during the flow: {e}")
        raise


# Entry point
if __name__ == "__main__":
    order_processing_flow()
