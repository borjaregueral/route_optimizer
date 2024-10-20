from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, TimestampType

def define_bronze_schema():
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_details", StructType([
            StructField("customer_id", StringType(), True),
            StructField("order_timestamp", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("items", ArrayType(
                StructType([
                    StructField("product_id", StringType(), True),
                    StructField("product_name", StringType(), True),
                    StructField("price", DoubleType(), True),
                    StructField("quantity", IntegerType(), True),
                    StructField("packages", ArrayType(
                        StructType([
                            StructField("package_id", StringType(), True),
                            StructField("subpackage_id", IntegerType(), True),
                            StructField("quantity", IntegerType(), True),
                            StructField("weight", DoubleType(), True),
                            StructField("volume", DoubleType(), True)
                        ])
                    ), True)
                ])
            ), True),
            StructField("total_amount", DoubleType(), True),
            StructField("total_volume", DoubleType(), True),
            StructField("total_weight", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("destination_address", StructType([
                StructField("address_id", StringType(), True),
                StructField("neighborhood", StringType(), True),
                StructField("coordinates", ArrayType(DoubleType()), True),
                StructField("road", StringType(), True),
                StructField("house_number", StringType(), True),
                StructField("suburb", StringType(), True),
                StructField("city_district", StringType(), True),
                StructField("state", StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("country", StringType(), True),
                StructField("lat", DoubleType(), True),
                StructField("lon", DoubleType(), True)
            ]), True),
            StructField("payment_details", StructType([
                StructField("payment_method", StringType(), True),
                StructField("payment_status", StringType(), True),
                StructField("transaction_id", StringType(), True)
            ]), True)
        ]), True)
    ])
    return schema

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, TimestampType

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, TimestampType

def define_silver_schema():
    """
    Defines the schema for the Silver Delta table, including the depot field.
    """
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("depot", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("order_date", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("total_volume", DoubleType(), True),
        StructField("total_weight", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("address_id", StringType(), True),
        StructField("neighborhood", StringType(), True),
        StructField("coordinates", ArrayType(DoubleType()), True),
        StructField("road", StringType(), True),
        StructField("house_number", StringType(), True),
        StructField("suburb", StringType(), True),
        StructField("city_district", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postcode", StringType(), True),
        StructField("country", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("items", ArrayType(
            StructType([
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("packages", ArrayType(
                    StructType([
                        StructField("package_id", StringType(), True),
                        StructField("subpackage_id", IntegerType(), True),
                        StructField("quantity", IntegerType(), True),
                        StructField("weight", DoubleType(), True),
                        StructField("volume", DoubleType(), True)
                    ])
                ), True)
            ])
        ), True)
    ])
    
    return schema



def define_gold_schema():
    """
    Defines the schema for the Gold Delta table, including the depot field.
    """
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("total_weight", DoubleType(), True),
        StructField("total_volume", DoubleType(), True),
        StructField("total_price", DoubleType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("hour", TimestampType(), True),  # Partitioning column for hour
        StructField("depot", StringType(), True)  # Adding depot column
    ])
    return schema
