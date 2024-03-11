from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import from_json, col
import os
from config import TOPIC

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-avro_2.12:3.3.1 pyspark-shell'

credentials_location = './docker/mage/google-cred.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "./data/gcs-connector-hadoop3-2.2.5.jar ") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)\

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

gcs_bucket_path = "gs://de-zoomcamp-project-data/"

df_raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", TOPIC ) \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", "checkpoint") \
        .load()

# Convert the 'value' column from binary to string
df_raw_stream = df_raw_stream.withColumn("value_str", df_raw_stream["value"].cast("string"))

# Define the schema of your JSON data
json_schema = "module_name string, module_id string, email string, time_homework string, time_lectures string, score string"

# Apply the transformation to create separate columns for each field
df_with_json = df_raw_stream.select("*",from_json(col("value_str"), json_schema).alias("value_json")).select("*","value_json.*").drop("value_str", "value_json")

# Selecting columns and casting their types
df_with_json = df_with_json.select(
    'module_name','module_id','email',
    col('time_homework').cast('float').alias('time_homework'),
    col('time_lectures').cast('float').alias('time_lectures'),
    col('score').cast('float').alias('score'),
    'timestamp','offset'
)

# def stop_query(query_name=None):
#     if query_name is None:
#         # Stop all queries
#         write_fact_query.stop()
#         write_time_query.stop()
#         # Add more queries here if needed
#     elif query_name == "write_fact_query":
#         write_fact_query.stop()
#     elif query_name == "write_time_query":
#         write_time_query.stop()
#     # Add more conditions for other query names if needed
#     else:
#         print("Invalid query name")

# # Example usage to stop a specific query
# stop_query("write_fact_query")  # Stop only the "write_fact_query" query

# # Example usage to stop all queries
# stop_query()  # Stop all queries


# Define a function to write a DataFrame to GCS
def write_to_gcs(df, batch_id, path):
    # Define the output path for this batch
    output_path = gcs_bucket_path + f"streaming_data/{path}/batch_{batch_id}/"
    # Write the batch to GCS
    df.write.format("parquet").mode("overwrite").save(output_path)

# Define your streaming query
fact_scores_query = df_with_json.select('email','module_id','score','timestamp','offset').writeStream \
    .foreachBatch(lambda df, batch_id: write_to_gcs(df, batch_id, 'fact_score')) \
    .start()

# Define another streaming query for the second DataFrame
fact_time_query = df_with_json.select('email','module_id','time_homewokr','time_lectures','timestamp','offset').writeStream \
    .foreachBatch(lambda df, batch_id: write_to_gcs(df, batch_id, 'fact_score')) \
    .start()

# Wait for the streaming queries to finish
fact_scores_query.awaitTermination()
fact_time_query.awaitTermination()