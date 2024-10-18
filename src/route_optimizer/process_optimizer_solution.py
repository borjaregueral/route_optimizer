import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_add
from pyspark import StorageLevel
from route_optimizer.awsmanager import AWSSessionManager
from datetime import datetime
from route_optimizer.config import load_config
from delta.tables import DeltaTable
from pyspark.sql.functions import date_format, to_timestamp, when, col, expr

# Load configuration
config = load_config()

# Set up logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DeltaTableBuilder:
    def __init__(self, spark: SparkSession, config: dict, aws_session, ideal_min_partitions=500, storage_level=StorageLevel.MEMORY_AND_DISK):
        self.spark = spark
        self.config = config
        self.s3_client = aws_session.s3_client
        self.ideal_min_partitions = ideal_min_partitions
        self.storage_level = storage_level
        self.input_path = config["optimized_files"]
        self.output_path = config["delta_table_path"]

    def load_json_files(self):
        logger.info(f"Loading JSON files from {self.input_path}")
        return self.spark.read.option("multiLine", "true").json(self.input_path)

    def transform_data(self, df):
        """
        Transform the data by adding a 'dispatched_hour' column, rounded to the closest 30 minutes, 
        and repartition dynamically. Uses 'timing.finishedProcessingAt' for the calculation.
        """
        try:
            logger.info("Transforming data by rounding 'dispatched_hour' to the nearest 30 minutes")
            ideal_partitions = max(df.rdd.getNumPartitions(), self.ideal_min_partitions)
            
            # Round to the nearest 30 minutes
            df_transformed = (df.withColumn("dispatched_hour", 
                                            when(col("timing.finishedProcessingAt").isNotNull(), 
                                                date_format(
                                                    expr("from_unixtime(round(unix_timestamp(to_timestamp(timing.finishedProcessingAt)) / 1800) * 1800)"), 
                                                    "yyyy-MM-dd-HH-mm"))
                                            .otherwise("UNKNOWN"))
                            .repartition(ideal_partitions)
                            .persist(self.storage_level))
            
            logger.info("Data transformed successfully")
            return df_transformed
        except Exception as e:
            logger.error(f"Error transforming data: {e}")
            raise

    def write_to_delta(self, df_transformed):
        """
        Append the transformed data to the Delta table, partitioning by 'dispatched_hour'.
        """
        try:
            logger.info(f"Appending transformed data to Delta table at {self.output_path}")
            
            # Check if the Delta table exists
            if DeltaTable.isDeltaTable(self.spark, self.output_path):
                # Append the new data
                (df_transformed.write 
                    .format("delta") 
                    .mode("append") 
                    .partitionBy("dispatched_hour") 
                    .save(self.output_path)
                )
                logger.info("Data appended to Delta table successfully")
            else:
                # If the table doesn't exist, create a new Delta table
                (df_transformed.write 
                    .format("delta") 
                    .mode("overwrite") 
                    .partitionBy("dispatched_hour") 
                    .save(self.output_path)
                )
                logger.info("New Delta table created and data written successfully")

        except Exception as e:
            logger.error(f"Error appending data to Delta table: {e}")
            raise

    def process_and_save(self):
        """
        Complete process: load, transform, and save the JSON files into a Delta table.
        """
        try:
            df = self.load_json_files()
            df_transformed = self.transform_data(df)
            self.write_to_delta(df_transformed)
            df_transformed.unpersist()
            
            logger.info("Process completed successfully")
        except Exception as e:
            logger.error(f"Error in process_and_save: {e}")
            raise
