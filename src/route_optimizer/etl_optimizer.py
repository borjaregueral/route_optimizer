import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum as spark_sum, ceil, lit, greatest
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.utils import AnalysisException

# Set up logger
logger = logging.getLogger(__name__)

class BronzeOptimizerAccumulator:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.gold_table_path = config["gold_table_path"]
        self.bronze_optimizer_table_path = config["bronze_optimizer_table_path"]
        self.silver_table_path = config["silver_optimizer_table_path"]
        self.max_volume_threshold = config["volume_threshold"]
        self.max_weight_threshold = config["weight_threshold"]
        self.max_time_window_minutes = config["time_threshold"]
        self.bronze_checkpoint_path = config["bronze_optimizer_checkpoint"]
        self.silver_checkpoint_path = config["silver_optimizer_checkpoint"]

    def orders_from_dispatcher_to_optimizer_bronze(self):
        """
        Process data from Gold to Bronze in streaming mode.
        """
        try:
            # Read from Gold table in streaming mode
            df_gold = self.spark.readStream.format("delta").load(self.gold_table_path)

            # Ensure 'order_timestamp' is properly typed as TimestampType
            df_gold = df_gold.withColumn("order_timestamp", col("order_timestamp").cast("timestamp"))

            # Change status to 'READY'
            df_bronze = df_gold.withColumn("status", lit("READY"))

            # Write to Bronze table in streaming mode
            query = df_bronze.writeStream.format("delta").outputMode("append") \
                .option("checkpointLocation", self.bronze_checkpoint_path) \
                .partitionBy("hour").start(self.bronze_optimizer_table_path)

            logger.info("Started streaming from Gold to Bronze.")

            query.awaitTermination(timeout=120)

        except Exception as e:
            logger.error(f"Error processing orders from Gold to Bronze: {e}")
            raise

    def process_orders_optimizer_from_bronze_to_silver(self):
        try:
            logger.info("Reading data from Bronze Delta table...")
            df_bronze = self.spark.read.format("delta").load(self.bronze_optimizer_table_path)

            # Ensure FIFO ordering by 'order_timestamp'
            df_bronze = df_bronze.orderBy("order_timestamp")

            # Define window specification partitioned by 'hour' and ordered by 'order_timestamp'
            window_spec = Window.partitionBy("hour").orderBy("order_timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)

            # Calculate cumulative sums of volume and weight
            df_bronze = df_bronze.withColumn("cum_volume", spark_sum("total_volume").over(window_spec))
            df_bronze = df_bronze.withColumn("cum_weight", spark_sum("total_weight").over(window_spec))

            # Compute batch numbers for volume and weight
            df_bronze = df_bronze.withColumn(
                "volume_batch",
                ceil(col("cum_volume") / self.max_volume_threshold)
            ).withColumn(
                "weight_batch",
                ceil(col("cum_weight") / self.max_weight_threshold)
            )

            # Assign batch_id as the maximum of volume_batch and weight_batch
            df_bronze = df_bronze.withColumn(
                "batch_id",
                greatest(col("volume_batch"), col("weight_batch"))
            )

            # Update status to 'READY_FOR_DISPATCH'
            df_bronze = df_bronze.withColumn("status", lit("READY_FOR_DISPATCH"))

            # Keep only the same columns as in Bronze, plus 'batch_id'
            columns_to_remove = ['cum_volume', 'cum_weight', 'volume_batch', 'weight_batch']
            # Remove 'batch_id' if it already exists in the DataFrame
            if 'batch_id' in df_bronze.columns:
                columns_to_remove.append('batch_id')

            selected_columns = [col_name for col_name in df_bronze.columns if col_name not in columns_to_remove] + ['batch_id']

            df_bronze = df_bronze.select(*selected_columns)

            # Write the processed data to the Silver table partitioned by 'hour'
            logger.info("Writing processed data to Silver Delta table...")
            df_bronze.write.format("delta").mode("append").partitionBy("hour").save(self.silver_table_path)

            logger.info("Successfully processed orders from Bronze to Silver.")

        except Exception as e:
            logger.error(f"Error in processing Bronze to Silver: {e}")
            raise


