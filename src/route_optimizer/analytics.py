import time
import logging
from typing import Any, Optional
import boto3

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AthenaManager:
    def __init__(self, athena_client: boto3.client, output_location: str):
        """
        Initialize AthenaManager with the Athena client and output location for query results.
        
        :param athena_client: Boto3 Athena client.
        :param output_location: S3 location for Athena query results.
        """
        self.athena_client = athena_client
        self.output_location = output_location

    def create_database(self, database_name: str) -> None:
        """
        Create a new Athena database if it doesn't exist.
        
        :param database_name: The name of the Athena database to create.
        """
        create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
        logger.info(f"Creating database: {database_name}")
        self.execute_athena_query(create_database_query, output_location=self.output_location)

    def create_table(self, create_table_query: str, database: str) -> None:
        """
        Create a new table in the Athena database.
        
        :param create_table_query: The SQL query to create the table.
        :param database: The Athena database in which to create the table.
        """
        logger.info(f"Creating table in database: {database}")
        self.execute_athena_query(create_table_query, database, self.output_location)

    def run_msck_repair(self, table_name: str, database: str) -> None:
        """
        Run MSCK REPAIR TABLE to update partitions in an Athena table.
        
        :param table_name: The name of the table.
        :param database: The Athena database.
        """
        repair_table_query = f"MSCK REPAIR TABLE {database}.{table_name};"
        logger.info(f"Running MSCK REPAIR TABLE on: {table_name}")
        self.execute_athena_query(repair_table_query, database, self.output_location)

    def execute_athena_query(self, query: str, database: Optional[str] = None, output_location: Optional[str] = None) -> str:
        """
        Execute an Athena query and wait for its completion.
        
        :param query: The SQL query to execute.
        :param database: The database context for the query.
        :param output_location: The S3 location for query results.
        :return: The query execution ID.
        """
        logger.info(f"Executing Athena query: {query}")
        
        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database} if database else None,
            ResultConfiguration={'OutputLocation': output_location or self.output_location}
        )

        query_execution_id = response['QueryExecutionId']
        status = 'RUNNING'

        # Wait for query execution to complete
        while status in ['RUNNING', 'QUEUED']:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            if status in ['FAILED', 'CANCELLED']:
                reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                logger.error(f"Query {query_execution_id} failed or was cancelled. Reason: {reason}")
                raise Exception(f"Query failed: {reason}")
            logger.info(f"Query status: {status}")
            time.sleep(2)

        logger.info(f"Query {query_execution_id} succeeded!")
        return query_execution_id

    def fetch_query_results(self, query_execution_id: str) -> None:
        """
        Fetch and print the results of the executed query.

        :param query_execution_id: The query execution ID.
        """
        paginator = self.athena_client.get_paginator('get_query_results')
        results_iterator = paginator.paginate(QueryExecutionId=query_execution_id)

        for results_page in results_iterator:
            for row in results_page['ResultSet']['Rows']:
                print([col.get('VarCharValue', '') for col in row['Data']])