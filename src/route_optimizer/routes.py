import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_add
from pyspark import StorageLevel
from route_optimizer.awsmanager import AWSSessionManager
from datetime import datetime
from route_optimizer.config import load_config
from delta.tables import DeltaTable
from pyspark.sql.functions import date_format, to_timestamp, when, col

# Load configuration
config = load_config()

# Set up logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class RoutesBuilder:
    def __init__(self, spark: SparkSession, config: dict, aws_session, ideal_min_partitions=500, storage_level=StorageLevel.MEMORY_AND_DISK):
        """
        Initialize the RoutesBuilder class with Spark session, configuration, AWS session,
        and paths for input and output from the config file.
        """
        self.spark = spark
        self.config = config
        self.s3_client = aws_session.s3_client
        self.ideal_min_partitions = ideal_min_partitions
        self.storage_level = storage_level
        self.input_path = config["delta_table_path"]  # Use the input Delta table path from config
        self.output_path = config["routes"]  # Use the output path from config

    def load_delta_table(self):
        """
        Load the Delta table from the given path.
        """
        try:
            logger.info(f"Loading Delta table from {self.input_path}")
            delta_table = DeltaTable.forPath(self.spark, self.input_path)
            return delta_table.toDF()  # Convert DeltaTable to DataFrame
        except Exception as e:
            logger.error(f"Error loading Delta table: {e}")
            raise

    def transform_data(self, df):
        """
        Transform the data by selecting 'input' and 'output' columns.
        """
        try:
            logger.info("Selecting 'input' and 'output' columns from the Delta table")
            df_transformed = df.select("input", "output", "dispatched_hour").persist(self.storage_level)
            logger.info("Data transformed successfully")
            return df_transformed
        except Exception as e:
            logger.error(f"Error transforming data: {e}")
            raise

    def write_to_delta(self, df_transformed):
        """
        Write the transformed data to a new Delta table.
        """
        try:
            logger.info(f"Writing transformed data to Delta table at {self.output_path}")
            
            # Append the new data with basePath option set
            (df_transformed.write 
                .format("delta") 
                .mode("overwrite") 
                .save(self.output_path)
            )
            logger.info("Data written to Delta table successfully")
        except Exception as e:
            logger.error(f"Error writing data to Delta table: {e}")
            raise

    def process_and_save(self):
        """
        Complete process: load, transform, and save the selected columns to a new Delta table.
        """
        try:
            df = self.load_delta_table()
            df_transformed = self.transform_data(df)
            self.write_to_delta(df_transformed)
            df_transformed.unpersist()
            logger.info("Process completed successfully")
        except Exception as e:
            logger.error(f"Error in process_and_save: {e}")
            raise