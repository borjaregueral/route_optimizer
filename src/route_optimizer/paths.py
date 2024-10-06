
# S3 paths
DISPATCHED_ORDERS_RAW_PATH = "s3a://dispatched-orders/optimized-dispatch-raw/"
DISPATCHED_ORDERS_BRONZE_PATH = "s3a://dispatched-orders/optimized-dispatch-bronze/"
DISPATCHED_ORDERS_SILVER_PATH = "s3a://dispatched-orders/optimized-dispatch-silver/"
DISPATCHED_ORDERS_GOLD_ROUTES_PATH = "s3a://dispatched-orders/optimized-dispatched-gold/routes/"
DISPATCHED_ORDERS_GOLD_PBI_PATH = "s3a://dispatched-orders/optimized-dispatched-gold/pBI/"

# Athena result locations
ATHENA_OUTPUT_LOCATION = "s3://your-athena-query-results/"

# Athena database and tables
ATHENA_DATABASE_NAME = "optimization_db"
DISPATCHED_ORDERS_TABLE_NAME = "dispatched_orders"
DISPATCHED_ORDERS_FLAT_VIEW = "dispatched_orders_flat"
DRIVER_SOLUTIONS_VIEW = "driver_solutions"

# Local paths (if needed)
LOCAL_DATA_PATH = "/path/to/local/data/"

# Routific paths
ROUTIFIC_API_URL = "https://api.routific.com/v1/vrp-long"

STREAM_NAME='OrderStreamForDispatching'
SHARD_ID='shardId-000000000000'
