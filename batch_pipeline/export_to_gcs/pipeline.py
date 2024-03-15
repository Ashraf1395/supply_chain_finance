import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from config import credentials_path, jar_file_path, gcs_bucket_path, output_path
from utils import customer_columns, product_columns, location_columns, order_columns, shipping_columns, department_columns, metadata_columns

# Spark configuration
conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", jar_file_path) \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_path) \
    .set("spark.hadoop.google.cloud.project.id", "gothic-sylph-387906")

# Create Spark Context
sc = SparkContext(conf=conf)

# Hadoop configuration for GCS access
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_path)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

# Create Spark Session
spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

# Load data from GCS bucket
print(f'Loading Data from GCS Bucket path {gcs_bucket_path}')
df_raw = spark.read.parquet(gcs_bucket_path + 'raw_streaming/*')
print('Done')

# Function to extract specific columns from DataFrame
def extract_columns(df, columns_to_extract):
    extracted_cols = [col("data").getItem(col_name).alias(col_name) for col_name in columns_to_extract]
    return df.select(*extracted_cols)

# Function to create dimension tables
def create_dimension_tables(df, extract_func, columns_to_extract, table_name):
    dimension_df = extract_func(df, columns_to_extract)
    # Uncomment the line below if you want to create a temporary view for SQL queries
    # dimension_df.createOrReplaceTempView(table_name)
    return dimension_df

# Create dimension tables
customer_dimension = create_dimension_tables(df_raw, extract_columns, customer_columns, "customer_dimension")
# customer_dimension.show()
print('Created Dimension table for Customers')

product_dimension = create_dimension_tables(df_raw, extract_columns, product_columns, "product_dimension")
# product_dimension.show()
print('Created Dimension table for Products')

location_dimension = create_dimension_tables(df_raw, extract_columns, location_columns, "location_dimension")
# location_dimension.show()
print('Created Dimension table for Location')

order_dimension = create_dimension_tables(df_raw, extract_columns, order_columns, "order_dimension").\
                    withColumnRenamed('Order date (DateOrders)','Order date')
# order_dimension.show()
print('Created Dimension table for Orders')

shipping_dimension = create_dimension_tables(df_raw, extract_columns, shipping_columns, "shipping_dimension").\
                        withColumnRenamed('Shipping date (DateOrders)','Shipping date').\
                        withColumnRenamed('Days for shipping (real)','Days for shipping real').\
                        withColumnRenamed('Days for shipment (scheduled)','Days for shipping scheduled')                     
shipping_dimension.show()
print('Created Dimension table for Shipping')

department_dimension = create_dimension_tables(df_raw, extract_columns, department_columns, "department_dimension")
# department_dimension.show()
print('Created Dimension table for Departments')

# Function to extract metadata columns
def extract_metadata(df, columns_to_extract):
    extracted_cols = [col("metadata").getItem(col_name).alias(col_name) for col_name in columns_to_extract]
    return df.select(*extracted_cols)

# Create metadata dimension table
metadata_dimension = create_dimension_tables(df_raw, extract_metadata, metadata_columns, "metadata_dimension")
# metadata_dimension.show()
print('Created Dimension table for Metadata')

# Dictionary to hold all dimension tables
dataframes = {
    "customer_dimension": customer_dimension,
    "product_dimension": product_dimension,
    "location_dimension": location_dimension,
    "order_dimension": order_dimension,
    "shipping_dimension": shipping_dimension,
    "department_dimension": department_dimension,
    "metadata_dimension": metadata_dimension
}

# Function to write dimension tables to GCS
def write_to_gcs(dataframes, output_path):
    print('Starting to Export Raw Streaming data to GCS...')
    for name, dataframe in dataframes.items():
        dataframe.write.mode("overwrite").option("header", "true").option("compression", "none").parquet(output_path + name + ".parquet")
        print(f"Exported Dataframe {name} to GCS.")

# Write dimension tables to GCS
write_to_gcs(dataframes, output_path)
