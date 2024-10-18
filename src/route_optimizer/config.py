# route_optimizer/config.py

import os
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()


def load_config():
    """
    Loads and returns the configuration for the application.
    """
    config = {
        "env_file": os.getenv("ENV_FILE", ".env"),
        "stream_name": os.getenv("STREAM_NAME", "OrderStreamForDispatching"),
        "bronze_table_path": os.getenv("BRONZE_TABLE_PATH", "s3a://orders-for-dispatch/bronze"),
        "silver_table_path": os.getenv("SILVER_TABLE_PATH", "s3a://orders-for-dispatch/silver"),
        "gold_table_path": os.getenv("GOLD_TABLE_PATH", "s3a://orders-for-dispatch/gold"),
        "bronze_checkpoint": os.getenv("BRONZE_CHECKPOINT", "s3a://orders-for-dispatch/checkpoint/bronze"),
        "silver_checkpoint": os.getenv("SILVER_CHECKPOINT", "s3a://orders-for-dispatch/checkpoint/silver"),
        "gold_checkpoint": os.getenv("GOLD_CHECKPOINT", "s3a://orders-for-dispatch/checkpoint/gold"),
        "bronze_optimizer_table_path": os.getenv("BRONZE_OPTIMIZER_TABLE_PATH", "s3a://dispatched-orders/bronze"),
        "silver_optimizer_table_path": os.getenv("SILVER_OPTIMIZER_TABLE_PATH", "s3a://dispatched-orders/silver/data"),
        "gold_optimizer_table_path": os.getenv("GOLD_OPTIMIZER_TABLE_PATH", "s3a://dispatched-orders/gold"),
        "bronze_optimizer_checkpoint": os.getenv("BRONZE_OPTIMIZER_CHECKPOINT", "s3a://dispatched-orders/checkpoint/bronze"),
        "silver_optimizer_checkpoint": os.getenv("SILVER_OPTIMIZER_CHECKPOINT", "s3a://dispatched-orders/checkpoint/silver"),
        "gold_optimizer_checkpoint": os.getenv("GOLD_OPTIMIZER_CHECKPOINT", "s3a://dispatched-orders/checkpoint/gold"),
        "to_optimizer_path": os.getenv("TO_OPTIMIZER", "silver"),
        "optimized_path": os.getenv("TO_OPTIMIZER_PATH", "gold/optimized"),
        "weight_threshold": float(os.getenv("WEIGHT_THRESHOLD", 1000)),
        "volume_threshold": float(os.getenv("VOLUME_THRESHOLD", 5000)),
        "time_threshold": int(os.getenv("TIME_THRESHOLD", 60 * 45)),  # 45 minutes
        "buffer_limit": int(os.getenv("BUFFER_LIMIT", 10000)),
        "batch_size": int(os.getenv("BATCH_SIZE", 10000)),
        "buffer_time": int(os.getenv("BUFFER_TIME", 360)),  # in seconds
        "shard_iterator_type": os.getenv("SHARD_ITERATOR_TYPE", 'LATEST'),
        "stream_reader_duration": int(os.getenv("STREAM_READER_DURATION", 5 * 60)),  # in seconds
        "max_iterations": int(os.getenv("MAX_ITERATIONS", 10000)),
        "depots": [
            {"id": "depot_1", "name": "San Sebastian de los Reyes", "lat": 40.54510, "lng": -3.61184},
            {"id": "depot_2", "name": "Alcorcón", "lat": 40.350370, "lng": -3.855863},
            {"id": "depot_3", "name": "Vallecas", "lat": 40.36977, "lng": -3.59670}
        ],
        # New Parameters for RouteOptimizer
        "routific_token": os.getenv("ROUTIFIC_TOKEN", ""),
        "routific_url": os.getenv("ROUTIFIC_URL", "https://api.routific.com/v1/vrp-long"),
        "routific_jobs": os.getenv("ROUTIFIC_JOBS", "https://api.routific.com/v1/jobs"),
        "optimization_bucket": os.getenv("OPTIMIZATION_BUCKET", "dispatched-orders"),
        "batching": os.getenv("BATCHING", "batching"),
        "dispatching_folder": os.getenv("DISPATCHING_FOLDER", "silver/batching"),
        #"silver_optimizer_folder": os.getenv("SILVER_OPTIMIZER_FOLDER", "to-optimizer"),
        "fleet_params": {
            "num_drivers": int(os.getenv("NUM_DRIVERS", 18)),
            "shift_start": os.getenv("SHIFT_START", "09:00"),
            "shift_end": os.getenv("SHIFT_END", "18:00"),
            "weight": float(os.getenv("FLEET_WEIGHT", 1000)),
            "volume": float(os.getenv("FLEET_VOLUME", 1000)),
            "drivers_per_depot": int(os.getenv("DRIVERS_PER_DEPOT", 6))
        },
        "visit_start_time": os.getenv("VISIT_START_TIME", "09:00"),
        "visit_end_time": os.getenv("VISIT_END_TIME", "18:00"),
        "visit_duration": int(os.getenv("VISIT_DURATION", 5)),
        "fleet_min_visits": int(os.getenv("FLEET_MIN_VISITS", 1)),
        "priorities": json.loads(os.getenv("PRIORITIES", '["low", "regular", "high"]')),
        "options": json.loads(os.getenv("ROUTING_OPTIONS", '{"traffic": "slow", "balance": true, "shortest_distance": true, "polylines": true}')),



        "processed_folder": os.getenv("PROCESSED_FOLDER", "s3a://dispatched-orders/silver/processed"),
        "optimized_files": os.getenv("OPTIMIZED_FILES", "s3a://dispatched-orders/gold/optimized"),
        "delta_table_path": os.getenv("DELTA_TABLE_PATH","s3a://dispatched-orders/gold/general"),
        "routes": os.getenv("ROUTES_PATH","s3a://dispatched-orders/gold/routes"),
        "pBI_data": os.getenv("PBI_DATA","s3a://dispatched-orders/gold/pBI/data/"),
        "pBI_data_athena": os.getenv("PBI_DATA_ATHENA","s3://dispatched-orders/gold/pBI/data"),
        "pBI_table": os.getenv("PBI_TABLES","s3a://dispatched-orders/gold/pBI/tables"),
        "pBI_table_athena": os.getenv("PBI_TABLES_ATHENA","s3://dispatched-orders/gold/pBI/tables"),
        "order_table": os.getenv("DISPATCHED_ORDERS_TABLES","s3://dispatched-orders/gold/pBI/dispatched_orders"),
        "athena_db_name": os.getenv("ATHENA_DB_NAME", 'dispatched_db'),  # Athena database name
        "athena_table_name": os.getenv("ATHENA_TABLE_NAME", 'data'),  # Athena table name
        "distances_table": os.getenv("DISTANCES_TABLE", "s3://dispatched-orders/gold/pBI/tables/distances/"),  # Table for distances
        "solution_table": os.getenv("SOLUTION_TABLE", "s3://dispatched-orders/gold/pBI/tables/solution/"),  # Table for solution
        "summary_table": os.getenv("SUMMARY_TABLE", "s3://dispatched-orders/gold/pBI/tables/summary/"),  # Table for general summar
        "view_path": os.getenv("VIEW_PATH", "s3://dispatched-orders/gold/pBI/tables/views/"),
    }


    return config

