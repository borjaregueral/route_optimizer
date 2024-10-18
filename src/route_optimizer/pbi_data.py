from pyspark.sql.functions import col, to_timestamp
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from route_optimizer.config import load_config
import logging

# Load configuration
config = load_config()

# Set up logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class PBITableProcessor:
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the PBITableProcessor class with Spark session and config parameters.
        
        :param spark: SparkSession object
        :param config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.pbi_parquet_path = config["pBI_data"]  # Path to store the cleaned Parquet files
        self.delta_path = config["delta_table_path"]  # Path to the Delta table

    def load_delta_table(self):
        """
        Load the Silver Delta table from the specified path.
        """
        try:
            logger.info(f"Loading Delta table from {self.delta_path}")
            delta_table = DeltaTable.forPath(self.spark, self.delta_path)
            return delta_table.toDF()  # Convert Delta table to DataFrame
        except Exception as e:
            logger.error(f"Error loading Delta table: {e}")
            raise

    def transform_data(self, df):
        """
        Transform the data by unnesting all fields in 'output' except for the fields
        'pl_precision' and 'polylines', and selecting required columns.
        
        :param df: Input DataFrame
        :return: Transformed DataFrame
        """
        try:
            logger.info("Transforming the data by unnesting all fields in 'output' except for excluded ones")

            # Get the list of fields to unnest from the 'output' struct, excluding 'pl_precision' and 'polylines'
            output_columns = [col(f"output.{field.name}").alias(f"output_{field.name}")
                              for field in df.schema["output"].dataType
                              if field.name not in ["pl_precision", "polylines"]]

            # Select the output columns and other required columns
            clean_df = df.select(*output_columns, "dispatched_hour", "finished_at")

            logger.info("Data transformed and output struct unnested successfully")
            return clean_df
        except Exception as e:
            logger.error(f"Error transforming data: {e}")
            raise

    def write_to_parquet(self, df_transformed):
        """
        Write the transformed DataFrame to Parquet, partitioned by 'dispatched_hour'.
        
        :param df_transformed: Transformed DataFrame
        """
        try:
            logger.info(f"Writing the transformed data to {self.pbi_parquet_path}, partitioned by 'dispatched_hour'")


            # Write the DataFrame to Parquet, partitioned by 'finished_at'
            (df_transformed
                            .write.mode('overwrite')
                            .partitionBy("finished_at")
                            .parquet(self.pbi_parquet_path)
            )

            logger.info("Data written to Parquet successfully")
        except Exception as e:
            logger.error(f"Error writing data to Parquet: {e}")
            raise

    def process_and_save(self):
        """
        Full process to load, transform, and save the Delta table as cleaned Parquet files.
        """
        try:
            df = self.load_delta_table()  # Load the Delta table instead of Parquet
            df_transformed = self.transform_data(df)
            self.write_to_parquet(df_transformed)
            logger.info("Process completed successfully")
        except Exception as e:
            logger.error(f"Error in process_and_save: {e}")
            raise
