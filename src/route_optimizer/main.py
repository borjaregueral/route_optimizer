import logging
from route_optimizer.awsmanager import AWSSessionManager
from route_optimizer.sparkmanager import SparkSessionManager
from route_optimizer.consumer import StreamReader
from route_optimizer.dispatcher import Dispatcher
from route_optimizer.config import load_config
from route_optimizer.schema import define_bronze_schema
from route_optimizer.etl_optimizer import BronzeOptimizerAccumulator
from route_optimizer.preprocessing_optimizer import EtlOptimizer  # Ensure the class is in optimizer.py
from route_optimizer.router import BatchOptimizer
from route_optimizer.process_optimizer_solution import DeltaTableBuilder
from route_optimizer.routes import RoutesBuilder  # Assuming the new class is in routes_builder.py
from route_optimizer.pbi_tables import PBITableProcessor
from prefect import task, flow

# Set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s')

# -------------------------------------------
# Tasks
# -------------------------------------------

@task(persist_result=False)
def initialize_aws_session(env_file):
    """
    Initializes the AWS session using the credentials from the env file.
    """
    try:
        aws_session = AWSSessionManager(env_file=env_file)
        logger.info("AWS Session Manager initialized.")
        return aws_session
    except Exception as e:
        logger.error(f"Error initializing AWS session: {e}")
        raise

@task(persist_result=False)
def initialize_spark_session(aws_credentials):
    """
    Initializes the Spark session with the provided AWS credentials.
    """
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
    """
    Starts the StreamReader to read data from Kinesis and write it to the Bronze Delta table.
    """
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
    """
    Task to process data from the Bronze to Silver Delta table.
    """
    try:
        dispatcher = Dispatcher(spark=spark, config=config)
        dispatcher.process_orders_to_silver()
    except Exception as e:
        logger.error(f"Error in processing from Bronze to Silver: {e}")
        raise

@task()
def process_silver_to_gold(config, spark):
    """
    Task to process data from the Silver to Gold Delta table.
    """
    try:
        dispatcher = Dispatcher(spark=spark, config=config)
        dispatcher.process_orders_to_gold()
    except Exception as e:
        logger.error(f"Error in processing from Silver to Gold: {e}")
        raise

@task()
def dispatcher_to_optimizer(config, spark):
    """
    Task to process data from the Gold to Bronze Optimizer.
    """
    try:
        accumulator = BronzeOptimizerAccumulator(spark, config)
        accumulator.orders_from_dispatcher_to_optimizer_bronze()
    except Exception as e:
        logger.error(f"Error in processing from Gold to Bronze Optimizer: {e}")
        raise

@task()
def process_orders_in_optimizer(config, spark):
    """
    Task to process orders in the Optimizer.
    """
    try:
        accumulator = BronzeOptimizerAccumulator(spark, config)
        accumulator.process_orders_optimizer_from_bronze_to_silver()
    except Exception as e:
        logger.error(f"Error in processing orders in the Optimizer: {e}")
        raise

@task()
def etl_optimizer(config, spark, aws_session):
    """
    Task to perform route optimization by utilizing the RouteOptimizer class.
    """
    try:
        optimizer = EtlOptimizer(spark=spark, config=config, aws_session=aws_session)
        optimizer.process_batches_and_optimize()
    except Exception as e:
        logger.error(f"Error during route optimization: {e}")
        raise

@task
def optimize_routes(config, aws_session):
    """
    Task to optimize routes using BatchOptimizer.
    """
    try:
        optimizer = BatchOptimizer(config, aws_session)
        optimizer.process_jobs()
    except Exception as e:
        logger.error(f"Error optimizing routes: {e}")
        raise

@task
def postprocessing(aws_session, spark, config):
    """
    Post-processing task using DeltaTableBuilder to handle analytics.
    """
    try:
        analytics_processor = DeltaTableBuilder(spark=spark, config=config, aws_session=aws_session)
        analytics_processor.process_and_save()
    except Exception as e:
        logger.error(f"Error during postprocessing: {e}")
        raise

@task
def process_routes(config, aws_session, spark):
    """
    Task to process routes using the new RoutesBuilder class.
    """
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

@task
def pBI_processor(config, spark):
    """
    Task to process routes using the new RoutesBuilder class.
    """
    try:
        pBI_builder = PBITableProcessor(
            spark=spark,
            config=config,

        )
        pBI_builder.process_and_save()
    except Exception as e:
        logger.error(f"Error processing routes: {e}")
        raise

# -------------------------------------------
# Flow
# -------------------------------------------

@flow(name="Order Processing Flow")
def order_processing_flow():
    """
    The main flow of the application which initializes AWS, Spark,
    starts the StreamReader, and processes data through Bronze, Silver, Gold, and Bronze Optimizer layers.
    """
    try:
        # Load configuration from config.py
        config = load_config()

        # Initialize AWS session
        aws_session = initialize_aws_session(config["env_file"])

        # Extract AWS credentials
        aws_credentials = aws_session.aws_credentials

        # Initialize Spark session
        spark = initialize_spark_session(aws_credentials)

        # Define the schema for Bronze (if needed)
        bronze_schema = define_bronze_schema()

        # # Start the StreamReader task to read from Kinesis and write to Bronze Delta table
        # try:
        #     start_stream_reader(config, aws_session, spark, bronze_schema)
        # except Exception as e:
        #     logger.error(f"Error in StreamReader task: {e}")
        #     raise

        # # Process from Bronze to Silver
        # try:
        #     process_bronze_to_silver(config, spark)
        # except Exception as e:
        #     logger.error(f"Error processing from Bronze to Silver: {e}")
        #     raise

        # # Process from Silver to Gold
        # try:
        #     process_silver_to_gold(config, spark)
        # except Exception as e:
        #     logger.error(f"Error processing from Silver to Gold: {e}")
        #     raise

        # # Process from Gold to Bronze Optimizer
        # try:
        #     dispatcher_to_optimizer(config, spark)
        # except Exception as e:
        #     logger.error(f"Error processing from Gold to Bronze Optimizer: {e}")
        #     raise

        # # Process from Bronze to Silver Optimizer
        # try:
        #     process_orders_in_optimizer(config, spark)
        # except Exception as e:
        #     logger.error(f"Error processing orders in Optimizer: {e}")
        #     raise

        # # Route optimization
        # try:
        #     etl_optimizer(config, spark, aws_session)
        # except Exception as e:
        #     logger.error(f"Error in route optimization: {e}")
        #     raise

        # # Optimize routes
        # try:
        #     optimize_routes(config, aws_session)
        # except Exception as e:
        #     logger.error(f"Error optimizing routes: {e}")
        #     raise

        # # Post-processing task for analytics
        # try:
        #     postprocessing(aws_session, spark, config)
        # except Exception as e:
        #     logger.error(f"Error during postprocessing: {e}")
        #     raise

        # # Process the routes using RoutesBuilder
        # try:
        #     process_routes(config, aws_session, spark)
        # except Exception as e:
        #     logger.error(f"Error processing routes: {e}")
        #     raise

        # Process the routes using RoutesBuilder
        try:
            pBI_processor(config, spark)
        except Exception as e:
            logger.error(f"Error processing routes: {e}")
            raise

    except Exception as e:
        logger.error(f"An error occurred in the order processing flow: {e}")
        raise

if __name__ == "__main__":
    order_processing_flow()
