from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, date_format
from dotenv import load_dotenv
import os

def main(): 
    load_dotenv()

    avro_jar_path = os.getenv("AVRO_DRIVER_PATH")

    # Initialize Spark session with Avro JAR included
    spark = SparkSession.builder \
        .appName("HTTP Log to Avro") \
        .config("spark.jars", avro_jar_path) \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.2") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    # Read the text file into a DataFrame
    df = spark.read.text("data/http_log.txt")

    # Split the lines and create new columns
    df_split = df.withColumn("split", split(col("value"), " ")) \
                .withColumn("timestamp", col("split").getItem(0)) \
                .withColumn("http_method", col("split").getItem(1)) \
                .withColumn("http_path", col("split").getItem(2)) \
                .withColumn("user_id", col("split").getItem(3)) \
                .drop("split", "value")

    df_formatted = df_split.withColumn("timestamp", date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

    # Write the DataFrame to Avro format
    df_formatted.write.format("avro").save("avro_data/http_log.avro")

    spark.stop()
if __name__ == "__main__":
    main()