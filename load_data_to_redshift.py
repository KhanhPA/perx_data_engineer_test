from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql.functions import date_format, col
import os
import boto3
import time

def load_environment_variables():
    # Load environment variables from .env file
    load_dotenv()

    # Return environment variables
    return {
        "redshift_url": os.getenv("REDSHIFT_URL"),
        "redshift_user": os.getenv("REDSHIFT_USER"),
        "redshift_password": os.getenv("REDSHIFT_PASSWORD"),
        "redshift_db_name": os.getenv("REDSHIFT_DB_NAME"),
        "redshift_cluster_id": os.getenv("REDSHIFT_CLUSTER_ID"),
        "s3_bucket": os.getenv("S3_BUCKET"),
        "aws_access_key": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "avro_jar_path": os.getenv("AVRO_DRIVER_PATH"),
        "aws_region": os.getenv("AWS_REGION"),
        "iam_role": os.getenv("REDSHIFT_IAM_ROLE")  
    }

def initialize_spark(avro_jar_path: str):
    # Initialize Spark session with the Avro driver
    spark = SparkSession.builder \
        .appName("RedshiftDataImport") \
        .config("spark.jars", avro_jar_path) \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-avro_2.12:3.5.2,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "org.apache.hadoop:hadoop-common:3.3.1") \
        .getOrCreate()

    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    return spark

def configure_hadoop(spark, aws_access_key: str, aws_secret_key: str):
    # Set Hadoop configuration to use AWS credentials
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_access_key)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

    # Enable the S3A committer to boost performance
    hadoop_conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    hadoop_conf.set("fs.s3a.committer.name", "directory")
    hadoop_conf.set("fs.s3a.committer.staging.tmp.path", "/tmp/staging")
    hadoop_conf.set("fs.s3a.committer.threads", "10")

def read_csv_data(spark, file_path: str, date_columns: list = None):
    # Load CSV data into DataFrame and format date columns if specified
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    if date_columns:
        for col_name in date_columns:
            df = df.withColumn(col_name, date_format(col(col_name), "yyyy-MM-dd HH:mm:ss"))
    return df

def write_data_to_s3(df, s3_path: str, format_type: str = "csv"):
    # Write DataFrame to S3
    df.write.format(format_type).option("header", True).mode("overwrite").save(s3_path)

def execute_redshift_copy_command(redshift_client, cluster_id: str, db_name: str, db_user: str, copy_query: str):
    # Execute Redshift COPY command
    try:
        response = redshift_client.execute_statement(
            ClusterIdentifier=cluster_id,
            Database=db_name,
            DbUser=db_user,
            Sql=copy_query
        )
        statement_id = response['Id']
        print(f"COPY command submitted. Statement ID: {statement_id}")
    except Exception as e:
        print(f"Error executing COPY command for {table_name}: {e}")

def main():
    # Load environment variables
    env_vars = load_environment_variables()
    
    # Initialize Spark session
    spark = initialize_spark(env_vars['avro_jar_path'])
    
    # Configure Hadoop for S3
    configure_hadoop(spark, env_vars['aws_access_key'], env_vars['aws_secret_key'])
    
    # Load csv data into DataFrames
    campaign_df = read_csv_data(spark, 'data/campaign.csv')
    reward_campaigns_df = read_csv_data(spark, 'data/reward_campaign.csv', ['updated_at'])
    reward_transactions_df = read_csv_data(spark, 'data/reward_transaction.csv', ['updated_at'])
    campaign_reward_mapping_df = read_csv_data(spark, 'data/campaign_reward_mapping.csv')
    http_log_df = spark.read.format("avro").load("avro_data/http_log.avro")

    # Define S3 directory paths using helper function
    s3_data_path_spark =  f"s3a://{env_vars['s3_bucket']}/data/"
    s3_data_path_redshift =  f"s3://{env_vars['s3_bucket']}/data/"

    # Write DataFrames to S3 for Spark 
    write_data_to_s3(campaign_df, f"{s3_data_path_spark}campaign/")
    write_data_to_s3(reward_campaigns_df, f"{s3_data_path_spark}reward_campaigns/")
    write_data_to_s3(reward_transactions_df, f"{s3_data_path_spark}reward_transactions/")
    write_data_to_s3(campaign_reward_mapping_df, f"{s3_data_path_spark}campaign_reward_mapping/")
    write_data_to_s3(http_log_df, f"{s3_data_path_spark}http_log/", "avro")

    # Initialize Redshift client
    redshift_client = boto3.client('redshift-data', 
                                   region_name=env_vars['aws_region'], 
                                   aws_access_key_id=env_vars['aws_access_key'],
                                   aws_secret_access_key=env_vars['aws_secret_key'])

    # Define COPY commands and execute them (s3://)
    copy_commands = [
        (f"{s3_data_path_redshift}http_log/", 'http_log', "AVRO"),
        (f"{s3_data_path_redshift}campaign/", 'campaign', 'CSV'),
        (f"{s3_data_path_redshift}reward_campaigns/", 'reward_campaign', 'CSV'),
        (f"{s3_data_path_redshift}reward_transactions/", 'reward_transaction', 'CSV'),
        (f"{s3_data_path_redshift}campaign_reward_mapping/", 'campaign_reward_mapping', 'CSV')
    ]

    for s3_path, table_name, file_format in copy_commands:
        if file_format.upper() == 'AVRO':
            copy_query = f"""
            COPY {table_name}
            FROM '{s3_path}'
            IAM_ROLE '{env_vars['iam_role']}'
            FORMAT AS AVRO 'auto';
            """
        elif file_format.upper() == 'CSV':
            copy_query = f"""
            COPY {table_name}
            FROM '{s3_path}'
            IAM_ROLE '{env_vars['iam_role']}'
            FORMAT AS CSV
            IGNOREHEADER 1
            DELIMITER ',';
            """
        print(f"Executing COPY command for {table_name}...")
        
        execute_redshift_copy_command(redshift_client, env_vars['redshift_cluster_id'], 
                                      env_vars['redshift_db_name'], env_vars['redshift_user'], copy_query)
    
    spark.stop()

if __name__ == "__main__":
    main()
