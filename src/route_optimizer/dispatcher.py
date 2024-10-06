# import time
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_timestamp
# from pyspark.sql.types import StructType
# from delta.tables import DeltaTable
# from datetime import datetime
# import logging

# logger = logging.getLogger(__name__)

# class Dispatcher:
#     def __init__(self, spark: SparkSession, schema: StructType, delta_table_path: str, dispatch_table_path: str, weight_threshold: float, volume_threshold: float, time_threshold: int, buffer_limit: int):
#         self.spark = spark
#         self.schema = schema
#         self.delta_table_path = delta_table_path
#         self.dispatch_table_path = dispatch_table_path
#         self.weight_threshold = weight_threshold
#         self.volume_threshold = volume_threshold
#         self.time_threshold = time_threshold
#         self.buffer_limit = buffer_limit

#         # Initialize buffer and flush time
#         self.order_buffer = []
#         self.last_flush_time = time.time()

#     def convert_timestamps(self, orders):
#         """
#         Convert the timestamps to proper datetime objects.
    
#         :param orders: List of orders.
#         :return: List of orders with converted timestamps.
#         """
#         for order in orders:
#             if isinstance(order['order_timestamp'], str):
#                 try:
#                     order['order_timestamp'] = datetime.strptime(order['order_timestamp'], "%Y-%m-%dT%H:%M:%SZ")
#                 except ValueError:
#                     try:
#                         order['order_timestamp'] = datetime.strptime(order['order_timestamp'], "%Y-%m-%d %H:%M:%S")
#                     except ValueError as e:
#                         logger.error(f"Timestamp format error: {str(e)}")
#                         raise
#         return orders

#     def process_batch(self, df, batch_id):
#         """
#         Process each micro-batch of data.
        
#         :param df: DataFrame containing the micro-batch of data.
#         :param batch_id: Batch ID.
#         """
#         orders = df.collect()
#         for order in orders:
#             self.order_buffer.append(order.asDict())
#             logger.info(f"Accumulated order: {order.asDict()}")

#         # Convert timestamps if necessary
#         self.order_buffer = self.convert_timestamps(self.order_buffer)

#         # Calculate accumulated weight and volume
#         total_weight = sum(order['total_weight'] for order in self.order_buffer)
#         total_volume = sum(order['total_volume'] for order in self.order_buffer)
#         time_elapsed = time.time() - self.last_flush_time

#         # Check if any thresholds are met (weight, volume, or time)
#         if total_weight >= self.weight_threshold or total_volume >= self.volume_threshold or time_elapsed >= self.time_threshold:
#             logger.info(f"Threshold met: Dispatching {len(self.order_buffer)} orders.")
            
#             # Update the status of all orders to 'READY_FOR_DISPATCH'
#             for order in self.order_buffer:
#                 order['status'] = 'READY_FOR_DISPATCH'

#             # Convert buffer to DataFrame and write to Delta table
#             df = self.spark.createDataFrame(self.order_buffer, schema=self.schema)
#             df = df.withColumn("order_timestamp", to_timestamp(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss"))
            
#             # Coalesce the DataFrame to reduce the number of partitions
#             df = df.coalesce(10)  # Adjust the number of partitions as needed
            
#             df.write.format("delta").mode("append").save(self.dispatch_table_path)
#             logger.info(f"Saved {len(self.order_buffer)} orders to Delta table.")
            
#             # Optimize the Delta table to compact small files
#             delta_table = DeltaTable.forPath(self.spark, self.dispatch_table_path)
#             delta_table.optimize().executeCompaction()
#             logger.info("Optimized the Delta table.")
            
#             # Clear the buffer and reset flush time
#             self.order_buffer.clear()
#             self.last_flush_time = time.time()

#     def read_and_dispatch_orders(self):
#         """
#         Reads orders from the Delta table, accumulates them, and processes them based on the defined thresholds.
#         """
#         try:
#             # Read the Delta table as a stream
#             df = self.spark.readStream.format("delta").load(self.delta_table_path)

#             # Use foreachBatch to process each micro-batch
#             query = df.writeStream.foreachBatch(self.process_batch).start()
#             query.awaitTermination()
#         except Exception as exc:
#             logger.error(f"An error occurred: {str(exc)}")
#             raise

# import time
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_timestamp
# from pyspark.sql.types import StructType
# from delta.tables import DeltaTable
# from datetime import datetime
# import logging

# logger = logging.getLogger(__name__)

# class Dispatcher:
#     def __init__(self, spark: SparkSession, schema: StructType, delta_table_path: str, dispatch_table_path: str, weight_threshold: float, volume_threshold: float, time_threshold: int, buffer_limit: int):
#         self.spark = spark
#         self.schema = schema
#         self.delta_table_path = delta_table_path
#         self.dispatch_table_path = dispatch_table_path
#         self.weight_threshold = weight_threshold
#         self.volume_threshold = volume_threshold
#         self.time_threshold = time_threshold
#         self.buffer_limit = buffer_limit

#         # Initialize buffer and flush time
#         self.order_buffer = []
#         self.last_flush_time = time.time()

#     def dispatch_orders(self):
#         # Calculate accumulated weight and volume
#         total_weight = sum(order['total_weight'] for order in self.order_buffer)
#         total_volume = sum(order['total_volume'] for order in self.order_buffer)
#         time_elapsed = time.time() - self.last_flush_time

#         # Check if any thresholds are met (weight, volume, or time)
#         if total_weight >= self.weight_threshold or total_volume >= self.volume_threshold or time_elapsed >= self.time_threshold:
#             logger.info(f"Threshold met: Dispatching {len(self.order_buffer)} orders.")
            
#             # Update the status of all orders to 'READY_FOR_DISPATCH'
#             for order in self.order_buffer:
#                 order['status'] = 'READY_FOR_DISPATCH'
#                 # Ensure order_timestamp is set to the current time if not already set
#                 if 'order_timestamp' not in order or order['order_timestamp'] is None:
#                     order['order_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#             # Convert buffer to DataFrame and write to Delta table
#             df = self.spark.createDataFrame(self.order_buffer, schema=self.schema)
            
#             # Ensure order_timestamp is correctly converted to timestamp
#             df = df.withColumn("order_timestamp", to_timestamp(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss"))
            
#             # Coalesce the DataFrame to reduce the number of partitions
#             df = df.coalesce(10)  # Adjust the number of partitions as needed
            
#             df.write.format("delta").mode("append").save(self.dispatch_table_path)
#             logger.info(f"Saved {len(self.order_buffer)} orders to Delta table.")
            
#             # Optimize the Delta table to compact small files
#             delta_table = DeltaTable.forPath(self.spark, self.dispatch_table_path)
#             delta_table.optimize().executeCompaction()
#             logger.info("Optimized the Delta table.")
            
#             # Clear the buffer and reset flush time
#             self.order_buffer.clear()
#             self.last_flush_time = time.time()

#     def read_and_dispatch_orders(self):
#         """
#         Reads orders from the Delta table, accumulates them, and processes them based on the defined thresholds.
#         """
#         try:
#             # Read the Delta table as a stream
#             df = self.spark.readStream.format("delta").load(self.delta_table_path)
#             # Process the stream (this part of the code should be implemented based on your specific requirements)
#         except Exception as e:
#             logger.error(f"Error reading and dispatching orders: {e}")
#             raise

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from delta.tables import DeltaTable
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class Dispatcher:
    def __init__(self, spark: SparkSession, schema: StructType, bronze_table_path: str, silver_table_path: str, weight_threshold: float, volume_threshold: float, time_threshold: int, buffer_limit: int):
        self.spark = spark
        self.schema = schema
        self.bronze_table_path = bronze_table_path
        self.silver_table_path = silver_table_path
        self.weight_threshold = weight_threshold
        self.volume_threshold = volume_threshold
        self.time_threshold = time_threshold
        self.buffer_limit = buffer_limit

        # Initialize buffer and flush time
        self.order_buffer = []
        self.last_flush_time = time.time()

    def read_orders_from_bronze(self):
        try:
            # Read the Delta table from the bronze path
            df = self.spark.read.format("delta").load(self.bronze_table_path)
            return df
        except Exception as e:
            logger.error(f"Error reading from bronze Delta table: {e}")
            raise

    def dispatch_orders(self):
        # Calculate accumulated weight and volume
        total_weight = sum(order['total_weight'] for order in self.order_buffer)
        total_volume = sum(order['total_volume'] for order in self.order_buffer)
        time_elapsed = time.time() - self.last_flush_time

        # Check if any thresholds are met (weight, volume, or time)
        if total_weight >= self.weight_threshold or total_volume >= self.volume_threshold or time_elapsed >= self.time_threshold:
            logger.info(f"Threshold met: Dispatching {len(self.order_buffer)} orders.")
            
            # Update the status of all orders to 'READY_FOR_DISPATCH'
            updated_orders = []
            for order in self.order_buffer:
                order_dict = order.asDict()
                order_dict['status'] = 'READY_FOR_DISPATCH'
                # Ensure order_timestamp is set to the current time if not already set
                if 'order_timestamp' not in order_dict or order_dict['order_timestamp'] is None:
                    order_dict['order_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                updated_orders.append(order_dict)

            # Convert buffer to DataFrame, ensure order_timestamp is correctly converted to timestamp, coalesce the DataFrame, and write to Delta table
            (self.spark.createDataFrame(updated_orders, schema=self.schema)
                 .withColumn("order_timestamp", to_timestamp(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss"))
                 .coalesce(10)  # Adjust the number of partitions as needed
                 .write.format("delta").mode("append").save(self.silver_table_path))


            logger.info(f"Saved {len(self.order_buffer)} orders to Delta table.")
            
            # Optimize the Delta table to compact small files
            delta_table = DeltaTable.forPath(self.spark, self.silver_table_path)
            delta_table.optimize().executeCompaction()
            logger.info("Optimized the Delta table.")
            
            # Clear the buffer and reset flush time
            self.order_buffer.clear()
            self.last_flush_time = time.time()

    def process_orders(self):
        """
        Reads orders from the bronze Delta table, accumulates them, and processes them based on the defined thresholds.
        """
        try:
            # Read orders from the bronze Delta table
            df = self.read_orders_from_bronze()
            
            # Collect the orders into the buffer
            self.order_buffer.extend(df.collect())
            
            # Dispatch orders if thresholds are met
            self.dispatch_orders()
        except Exception as e:
            logger.error(f"Error processing orders: {e}")
            raise