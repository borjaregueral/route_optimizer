import logging
import time
from route_optimizer.config import load_config
from pyspark.sql import SparkSession
from route_optimizer.awsmanager import AWSSessionManager
from route_optimizer.sparkmanager import SparkSessionManager

# Load configuration
config = load_config()

# Set up logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class PBIAthenaTableManager:
    def __init__(self, aws_session: AWSSessionManager, config, spark: SparkSessionManager):
        """
        Initialize the PBIAthenaTableManager class with Athena client and config.

        :param athena_client: AWS Athena client from boto3
        :param config: Configuration dictionary
        """
        self.athena_client = aws_session.athena_client
        #self.s3_client = aws_session.s3_client
        self.config = config
        self.spark = spark 
        self.s3_output_location = config["pBI_table_athena"]
        self.pbi_parquet_athena = config["pBI_data_athena"]
        self.pbi_parquet_spark = config["pBI_data"]
        self.db_name = config["athena_db_name"]
        self.table_name = config["athena_table_name"]

    def create_database(self):
        """
        Create the database in Athena if it does not exist.
        """
        create_database_query = f"""
        CREATE DATABASE IF NOT EXISTS {self.db_name}
        """
        response = self.athena_client.start_query_execution(
            QueryString=create_database_query,
            ResultConfiguration={'OutputLocation': self.s3_output_location}
        )
        query_execution_id = response['QueryExecutionId']
        self.wait_for_query_completion(query_execution_id)
        logger.info(f"Database {self.db_name} created successfully.")

    def create_dispatched_orders_table(self):
        """
        Create the 'dispatched_orders' table in Athena with the proper structure.
        """
        # create_table_query = f"""
        # CREATE EXTERNAL TABLE IF NOT EXISTS {self.db_name}.{self.table_name} (
        #     output_distances STRUCT<
        #         driver_1: DOUBLE,
        #         driver_2: DOUBLE,
        #         driver_3: DOUBLE,
        #         driver_4: DOUBLE,
        #         driver_5: DOUBLE,
        #         driver_6: DOUBLE,
        #         driver_7: DOUBLE,
        #         driver_8: DOUBLE,
        #         driver_9: DOUBLE,
        #         driver_10: DOUBLE,
        #         driver_11: DOUBLE,
        #         driver_12: DOUBLE,
        #         driver_13: DOUBLE,
        #         driver_14: DOUBLE,
        #         driver_15: DOUBLE,
        #         driver_16: DOUBLE,
        #         driver_17: DOUBLE,
        #         driver_18: DOUBLE
        #     >,
        #     output_num_late_visits BIGINT,
        #     output_num_unserved BIGINT,
        #     output_solution STRUCT<
        #         driver_1: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_2: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_3: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_4: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_5: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_6: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_7: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_8: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_9: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_10: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_11: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_12: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_13: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_14: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_15: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_16: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_17: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>,
        #         driver_18: ARRAY<STRUCT<
        #             arrival_time: STRING,
        #             distance: DOUBLE,
        #             finish_time: STRING,
        #             location_id: STRING,
        #             location_name: STRING
        #         >>
        #     >,
        #     output_status STRING,
        #     output_total_break_time BIGINT,
        #     output_total_distance DOUBLE,
        #     output_total_idle_time BIGINT,
        #     output_total_travel_time BIGINT,
        #     output_total_vehicle_overtime BIGINT,
        #     output_total_visit_lateness BIGINT,
        #     output_total_working_time BIGINT,
        #     output_unserved STRING,
        #     output_vehicle_overtime STRUCT<
        #         driver_1: BIGINT,
        #         driver_2: BIGINT,
        #         driver_3: BIGINT,
        #         driver_4: BIGINT,
        #         driver_5: BIGINT,
        #         driver_6: BIGINT,
        #         driver_7: BIGINT,
        #         driver_8: BIGINT,
        #         driver_9: BIGINT,
        #         driver_10: BIGINT,
        #         driver_11: BIGINT,
        #         driver_12: BIGINT,
        #         driver_13: BIGINT,
        #         driver_14: BIGINT,
        #         driver_15: BIGINT,
        #         driver_16: BIGINT,
        #         driver_17: BIGINT,
        #         driver_18: BIGINT
        #     >,
        #     dispatched_hour STRING
        # )
        # PARTITIONED BY (finished_at STRING)
        # STORED AS PARQUET
        # LOCATION '{self.pbi_parquet_athena}'
        # TBLPROPERTIES ('parquet.compress'='SNAPPY');
        # """
        # # Execute the Athena query to create the table
        # response = self.athena_client.start_query_execution(
        #     QueryString=create_table_query,
        #     QueryExecutionContext={'Database': self.db_name},
        #     ResultConfiguration={'OutputLocation': self.s3_output_location}
        # )
        # query_execution_id = response['QueryExecutionId']
        # self.wait_for_query_completion(query_execution_id)
        # logger.info(f"Table '{self.table_name}' created successfully.")
        pass

    def repair_dispatched_orders_table(self):
        """
        Run MSCK REPAIR TABLE to load partitions for the dispatched_orders table.
        """
        repair_table_query = f"MSCK REPAIR TABLE {self.db_name}.{self.table_name};"
        response = self.athena_client.start_query_execution(
            QueryString=repair_table_query,
            QueryExecutionContext={'Database': self.db_name},
            ResultConfiguration={'OutputLocation': self.s3_output_location}
        )

        query_execution_id = response['QueryExecutionId']
        status = 'RUNNING'

        while status in ['RUNNING', 'QUEUED']:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            if status in ['FAILED', 'CANCELLED']:
                reason = response['QueryExecution']['Status']['StateChangeReason']
                raise Exception(f"MSCK REPAIR TABLE query {query_execution_id} failed or was cancelled. Reason: {reason}")
            logger.info(f"MSCK REPAIR TABLE status: {status}")
            time.sleep(2)

        logger.info(f"MSCK REPAIR TABLE {query_execution_id} succeeded!")
        logger.info(f"MSCK REPAIR TABLE for {self.table_name} succeeded.")

    def wait_for_query_completion(self, query_execution_id):
        """
        Wait for the Athena query to complete by checking the status.
        """
        status = 'RUNNING'
        while status == 'RUNNING':
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            logger.info(f"Query status: {status}")
            if status in ['FAILED', 'CANCELLED']:
                raise Exception(f"Query {query_execution_id} failed or was cancelled")
            time.sleep(2)
        logger.info(f"Query {query_execution_id} completed successfully.")

    def create_views_by_finished_at(self, df):
        """
        Create Athena views for each unique 'finished_at' value.

        :param df: The DataFrame containing 'finished_at' column.
        """
        # Get distinct 'finished_at' values
        unique_finished_at_values = df.select("finished_at").distinct().collect()

        for row in unique_finished_at_values:
            finished_at_value = row['finished_at']
            
            # Clean the 'finished_at' value to make it a valid view name
            clean_finished_at_value = finished_at_value.replace('-', '_').replace(':', '_').replace('.', '_')

            view_name = f"dispatched_orders_{clean_finished_at_value}"
            
            # Create a view per cleaned finished_at value
            create_view_query = f"""
            CREATE OR REPLACE VIEW {self.db_name}.{view_name} AS
            SELECT * FROM {self.db_name}.{self.table_name}
            WHERE finished_at = '{finished_at_value}';
            """
            
            # Execute the Athena query to create the view
            response = self.athena_client.start_query_execution(
                QueryString=create_view_query,
                QueryExecutionContext={'Database': self.db_name},
                ResultConfiguration={'OutputLocation': self.s3_output_location}
            )
            query_execution_id = response['QueryExecutionId']
            self.wait_for_query_completion(query_execution_id)
            logger.info(f"View '{view_name}' created for finished_at: {finished_at_value}")

    def execute_athena_query(self, query):
        """
        Execute the Athena query and wait for it to complete.
        """
        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': self.db_name
            },
            ResultConfiguration={
                'OutputLocation': self.s3_output_location
            }
        )
        query_execution_id = response['QueryExecutionId']
        status = 'RUNNING'

        while status in ['RUNNING', 'QUEUED']:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            if status in ['FAILED', 'CANCELLED']:
                reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                raise Exception(f"Query {query_execution_id} failed or was cancelled. Reason: {reason}")
            logger.info(f"Query status: {status}")
            time.sleep(2)

        logger.info(f"Query {query_execution_id} succeeded!")
        return query_execution_id

    def create_driver_solutions_view(self):
        """
        Build and execute the query to create the driver_solutions view in Athena.
        """
        # Build the query for driver_solutions view
        driver_solutions_query = """
        CREATE OR REPLACE VIEW optimization_db.driver_solutions AS
        """

        # Generate SELECT statements for each driver
        for i in range(1, 19):
            driver_id = f'driver_{i}'
            select_statement = f"""
            SELECT
              dispatched_date,
              finished_at,
              '{driver_id}' AS driver_id,
              t.elem.arrival_time,
              t.elem.distance,
              t.elem.finish_time,
              t.elem.location_id,
              t.elem.location_name
            FROM optimization_db.dispatched_orders
            CROSS JOIN UNNEST(output_solution.{driver_id}) AS t (elem)
            """
            if i > 1:
                driver_solutions_query += "\nUNION ALL\n"
            driver_solutions_query += select_statement

        logger.info("Creating driver_solutions view...")
        self.execute_athena_query(driver_solutions_query)
        logger.info("driver_solutions view created successfully.")

    def query_driver_solutions(self, driver_id):
        """
        Query the driver_solutions view for a specific driver.
        """
        query = f"""
        SELECT *
        FROM optimization_db.driver_solutions
        WHERE driver_id = '{driver_id}'
        ORDER BY finished_at, arrival_time
        LIMIT 20;
        """

        logger.info(f"Running query for {driver_id}...")
        query_execution_id = self.execute_athena_query(query)

        # Fetch and display results
        results_paginator = self.athena_client.get_paginator('get_query_results')
        results_iterator = results_paginator.paginate(QueryExecutionId=query_execution_id)

        results = []
        for results_page in results_iterator:
            for row in results_page['ResultSet']['Rows']:
                results.append([col.get('VarCharValue', '') for col in row['Data']])

        logger.info(f"Results for {driver_id}: {results}")
        return results

    def query_all_drivers(self):
        """
        Query the driver_solutions view for all drivers (driver_1 to driver_18).
        """
        for i in range(1, 19):
            driver_id = f'driver_{i}'
            try:
                self.query_driver_solutions(driver_id)
            except Exception as e:
                logger.error(f"Error querying {driver_id}: {e}")

    def process_pbi_data(self):
        """
        Orchestrate the process of creating the database, table, running MSCK REPAIR TABLE, and creating views.
        """
        if self.spark is None:
            logger.error("Spark session is not initialized")
            raise RuntimeError("Spark session is not initialized")
        try:
            # Create the database if it doesn't exist
            self.create_database()

            # Create the dispatched orders table
            self.create_dispatched_orders_table()

            # Run MSCK REPAIR TABLE to load partitions
            self.repair_dispatched_orders_table()

            # Read data from the parquet path
            df = self.spark.read.format("parquet").load(self.pbi_parquet_spark)

            # Create views for each unique 'finished_at' timestamp
            self.create_views_by_finished_at(df)

            # Create the driver_solutions view
            self.create_driver_solutions_view()

            # Query the driver_solutions view for all drivers
            self.query_all_drivers()

            logger.info("PBI dispatched orders table, partitions loaded, and views created successfully.")
        
        except Exception as e:
            logger.error(f"Error in processing PBI data: {e}")
            raise
