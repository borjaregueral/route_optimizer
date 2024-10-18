import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, udf, date_trunc
from pyspark.sql.types import StringType, StructType
from prefect import task, flow
from route_optimizer.awsmanager import AWSSessionManager
from route_optimizer.sparkmanager import SparkSessionManager
from route_optimizer.consumer import StreamReader
from route_optimizer.schema import define_bronze_schema, define_silver_schema, define_gold_schema  # Placeholder for your schema definitions
from route_optimizer.config import load_config
from route_optimizer.schema import define_silver_schema  # Assuming your schema is defined here
from math import fabs  # For Manhattan distance

# Set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s')


class Dispatcher:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.bronze_table = config["bronze_table_path"]
        self.silver_table = config["silver_table_path"]
        self.gold_table = config["gold_table_path"]
        self.silver_checkpoint = config["silver_checkpoint"]
        self.gold_checkpoint = config["gold_checkpoint"]
        self.depots = config["depots"]

    def read_delta_table(self, table_path):
        """
        Helper function to read a Delta table in streaming mode.
        """
        try:
            return self.spark.readStream.format("delta").load(table_path)
        except Exception as e:
            logger.error(f"Error reading Delta table from {table_path}: {e}")
            raise

    def flatten_schema_columns(self, schema, prefix=""):
        """
        Recursively flattens nested struct fields in the schema.
        Handles multiple levels of nesting.
        If it's a struct field, it adds its children prefixed by the parent name.
        If it's an array, it keeps the array structure.
        """
        fields = []
        for field in schema:
            field_name = f"{prefix}.{field.name}" if prefix else field.name
            if isinstance(field.dataType, StructType):
                # Recursively flatten the nested structure
                fields += self.flatten_schema_columns(field.dataType, field_name)
            else:
                # Add the flattened field to the list
                fields.append(col(field_name).alias(field_name.replace(".", "_")))
        return fields

    def process_orders_to_silver(self):
        try:
            # Read from Bronze Delta table
            df_bronze_stream = self.read_delta_table(self.bronze_table)

            # Broadcast depots to all worker nodes
            depots_broadcast = self.spark.sparkContext.broadcast(self.depots)

            # UDF to calculate the nearest depot using Manhattan distance
            calculate_nearest_depot_udf = udf(lambda lat, lon: calculate_nearest_depot(lat, lon, depots_broadcast.value), StringType())

            # Get the schema from the Bronze definition
            bronze_schema = define_bronze_schema()

            # Flatten the bronze schema, handling multiple levels of nesting
            flat_columns = self.flatten_schema_columns(bronze_schema)

            # Apply column selection and flattening dynamically
            df_silver_stream = (
                df_bronze_stream
                .select(flat_columns)  # Flatten the schema using the helper function
                .withColumn("lat", col("order_details_destination_address_lat"))
                .withColumn("lon", col("order_details_destination_address_lon"))
                .withColumn("depot", calculate_nearest_depot_udf(col("lat"), col("lon")))  # Adding depot calculation
                .withColumn("order_timestamp", to_timestamp(col("order_details_order_timestamp"), "yyyy-MM-dd HH:mm:ss"))
                .drop("order_details_destination_address")  # Drop the deeply nested address structure
            )

            # Write to Silver Delta table with a 30-second watermark and append mode
            write =(df_silver_stream
                .withWatermark("order_timestamp", "30 seconds")
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("path", self.silver_table)
                .option("checkpointLocation", self.silver_checkpoint)
                .start()
                .awaitTermination(timeout=300))
            
        except Exception as e:
            logger.error(f"Error processing orders to Silver: {e}")
            raise

    def process_orders_to_gold(self):
            try:
                # Read from Silver Delta table
                df_silver_stream = self.read_delta_table(self.silver_table)

                # Filter for paid orders and correct column names for the flattened schema
                df_gold_stream = (
                    df_silver_stream
                    .filter(col("order_details_payment_details_payment_status") == "PAID")  # Filter for only paid orders
                    .select(
                        col("order_id"),
                        col("order_details_customer_id").alias("customer_id"),
                        col("order_details_total_weight").alias("total_weight"),
                        col("order_details_total_volume").alias("total_volume"),
                        col("order_details_total_amount").alias("total_price"),  # Alias total_amount as total_price
                        col("order_details_order_timestamp").alias("order_timestamp"),
                        col("order_details_status").alias("status"),
                        col("lat"),
                        col("lon"),
                        col("depot"),
                        # Add partition column by extracting the hour from order_timestamp
                        date_trunc("hour", col("order_details_order_timestamp")).alias("hour")
                    )
                )

                # Write to Gold Delta table
                (df_gold_stream
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("path", self.gold_table)
                .option("checkpointLocation", self.gold_checkpoint)
                .partitionBy("hour")  # Partition by the hour
                .start()
                .awaitTermination(timeout=300))
            except Exception as e:
                logger.error(f"Error processing orders to Gold: {e}")
                raise


def calculate_nearest_depot(lat, lon, depots):
    """
    Calculate the nearest depot using Manhattan distance.
    Manhattan distance formula: |x1 - x2| + |y1 - y2|
    """
    nearest_depot = None
    min_distance = float('inf')

    for depot in depots:
        depot_lat = depot['lat']
        depot_lon = depot['lng']
        distance = fabs(lat - depot_lat) + fabs(lon - depot_lon)

        if distance < min_distance:
            min_distance = distance
            nearest_depot = depot['name']

    return nearest_depot

