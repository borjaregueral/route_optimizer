import os
import logging
import boto3
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class AWSSessionManager:
    def __init__(self, env_file: str):
        """
        Initialize the AWS session manager with environment variables.
        """
        self.env_file = env_file
        self.aws_credentials = self.load_env_variables()
        self.kinesis_client = self.initialize_kinesis_client()
        self.s3_client = self.initialize_s3_client()
        self.athena_client = self.initialize_athena_client()

    def load_env_variables(self) -> dict:
        """
        Load environment variables from a .env file.

        :return: A dictionary containing AWS credentials.
        """
        try:
            load_dotenv(self.env_file)
            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
            aws_session_token = os.getenv("AWS_SESSION_TOKEN")
            aws_region = os.getenv("AWS_REGION")

            return {
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key,
                "aws_session_token": aws_session_token,
                "aws_region": aws_region
            }

        except Exception as e:
            logger.error(f"Error loading environment variables: {str(e)}")
            raise

    def initialize_kinesis_client(self) -> boto3.client:
        """
        Initialize the boto3 Kinesis client with the AWS credentials.

        :return: A boto3 Kinesis client instance.
        """
        try:
            client = boto3.client(
                'kinesis',
                aws_access_key_id=self.aws_credentials['aws_access_key_id'],
                aws_secret_access_key=self.aws_credentials['aws_secret_access_key'],
                aws_session_token=self.aws_credentials['aws_session_token'],
                region_name=self.aws_credentials['aws_region']
            )
            logger.info("Kinesis client initialized successfully.")
            return client
        except Exception as e:
            logger.error(f"Error initializing Kinesis client: {str(e)}")
            raise

    def initialize_s3_client(self) -> boto3.client:
        """
        Initialize the boto3 S3 client with the AWS credentials.

        :return: A boto3 S3 client instance.
        """
        try:
            client = boto3.client(
                's3',
                aws_access_key_id=self.aws_credentials['aws_access_key_id'],
                aws_secret_access_key=self.aws_credentials['aws_secret_access_key'],
                aws_session_token=self.aws_credentials['aws_session_token'],
                region_name=self.aws_credentials['aws_region']
            )
            logger.info("S3 client initialized successfully.")
            return client
        except Exception as e:
            logger.error(f"Error initializing S3 client: {str(e)}")
            raise

    def initialize_athena_client(self) -> boto3.client:
        """
        Initialize the boto3 S3 client with the AWS credentials.

        :return: A boto3 S3 client instance.
        """
        try:
            client = boto3.client(
                'athena',
                aws_access_key_id=self.aws_credentials['aws_access_key_id'],
                aws_secret_access_key=self.aws_credentials['aws_secret_access_key'],
                aws_session_token=self.aws_credentials['aws_session_token'],
                region_name=self.aws_credentials['aws_region']
            )
            logger.info("S3 client initialized successfully.")
            return client
        except Exception as e:
            logger.error(f"Error initializing S3 client: {str(e)}")
            raise

    def refresh_session(self):
        """
        Refresh the AWS session by reloading the environment variables and reinitializing the Kinesis and S3 clients.
        """
        try:
            self.aws_credentials = self.load_env_variables()
            self.kinesis_client = self.initialize_kinesis_client()
            self.s3_client = self.initialize_s3_client()
            self.athena_client = self.initialize_athena_client()
            
            logger.info("AWS session refreshed successfully.")
        except Exception as e:
            logger.error(f"Error refreshing AWS session: {str(e)}")
            raise