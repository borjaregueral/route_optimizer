import logging
from pyspark.sql import SparkSession
from typing import Optional

logger = logging.getLogger(__name__)

class SparkSessionManager:
    def __init__(self, aws_credentials: dict, local_jars: Optional[str] = None):
        self.aws_credentials = aws_credentials
        self.spark = None  # Initialize the spark attribute
        self.local_jars = local_jars or "/Users/borja/Documents/Somniumrema/projects/de/route_optimizer/jars/aws-java-sdk-kinesis-1.12.364.jar"
        self.spark = self.initialize_spark_session(app_name="RouteOptimizer")

    def initialize_spark_session(
        self,
        app_name: str,
        driver_memory: str = "4g",
        executor_memory: str = "4g",
        executor_cores: str = "16",
        executor_instances: str = "16",
        memory_overhead: str = "4g",
        shuffle_partitions: str = "500",
        connection_timeout: str = "5000",
        max_attempts: str = "20",
        connection_max: str = "500",
        metrics_reporting: str = "false",
        broadcast_timeout: str = "600",
        auto_broadcast_threshold: str = "100MB",
        max_to_string_fields: str = "1000",
        multipart_size: str = "104857600",
        fast_upload: str = "true",
        fast_upload_buffer: str = "disk",
        fast_upload_active_blocks: str = "4",
        fast_upload_active_blocks_threshold: str = "4096",
        fast_upload_buffer_size: str = "2048576"
    ) -> SparkSession:
        """
        Initialize Spark session with Delta and S3 settings, logging errors if any occur.
        :param app_name: Name of the Spark application.
        :return: A configured Spark session.
        """
        if self.spark is None:
            try:
                self.spark = (
                    SparkSession.builder
                    .appName(app_name)
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026")
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.hadoop.fs.s3a.access.key", self.aws_credentials["aws_access_key_id"])
                    .config("spark.hadoop.fs.s3a.secret.key", self.aws_credentials["aws_secret_access_key"])
                    .config("spark.hadoop.fs.s3a.session.token", self.aws_credentials["aws_session_token"])
                    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
                    .config("spark.driver.memory", driver_memory)
                    .config("spark.executor.memory", executor_memory)
                    .config("spark.executor.memoryOverhead", memory_overhead)
                    .config("spark.executor.instances", executor_instances)
                    .config("spark.executor.cores", executor_cores)
                    .config("spark.sql.shuffle.partitions", shuffle_partitions)
                    .config("spark.hadoop.fs.s3a.connection.timeout", connection_timeout)
                    .config("spark.hadoop.fs.s3a.attempts.maximum", max_attempts)
                    .config("spark.hadoop.fs.s3a.connection.maximum", connection_max) 
                    .config("spark.hadoop.fs.s3a.metrics.reporting", metrics_reporting) 
                    .config("spark.sql.broadcastTimeout", broadcast_timeout) 
                    .config("spark.sql.autoBroadcastJoinThreshold", auto_broadcast_threshold) 
                    .config("spark.sql.debug.maxToStringFields", max_to_string_fields) 
                    .config("spark.hadoop.fs.s3a.multipart.size", multipart_size) 
                    .config("spark.hadoop.fs.s3a.fast.upload", fast_upload) 
                    .config("spark.hadoop.fs.s3a.fast.upload.buffer", fast_upload_buffer) 
                    .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", fast_upload_active_blocks) 
                    .config("spark.hadoop.fs.s3a.fast.upload.active.blocks.threshold", fast_upload_active_blocks_threshold) 
                    .config("spark.hadoop.fs.s3a.fast.upload.buffer.size", fast_upload_buffer_size) 
                    .config("spark.sql.adaptive.enabled", "false")
                    .getOrCreate()
                )

                self.spark.sparkContext.setLogLevel("WARN")
                logger.info(f"Spark session initialized successfully for app '{app_name}'")
            except Exception as e:
                logger.error(f"Error initializing Spark session: {str(e)}")
                raise

        return self.spark
