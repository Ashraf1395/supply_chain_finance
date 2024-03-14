import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, monotonically_increasing_id, array_contains, col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from config import credentials_path,jar_file_path,gcs_bucket_path,output_path
from utils import customer_columns,product_columns,location_columns,order_columns,shipping_columns,department_columns,metadata_columns
# credentials_location = './docker/mage/google-cred.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", jar_file_path) \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_path)\
    .set("spark.hadoop.google.cloud.project.id", "gothic-sylph-387906")

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_path)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

# gcs_bucket_path = "gs://supply-chain-data/"

print(f'Loading Data from GCS Bucket path {gcs_bucket_path}')
df_raw = spark.read.parquet(gcs_bucket_path + 'raw_streaming/*')

def extract_columns(df, columns_to_extract):
    extracted_cols = [col("data").getItem(col_name).alias(col_name) for col_name in columns_to_extract]
    return df.select(*extracted_cols)

def create_dimension_tables(df, extract_func, columns_to_extract, table_name):
    dimension_df = extract_func(df, columns_to_extract)
    # dimension_df.createOrReplaceTempView(table_name)
    return dimension_df

# Create dimension tables
print('Creating Dimension table for Customers')

# customer_columns = ["Customer Id", "Customer Email", "Customer Fname", "Customer Lname",
#                     "Customer Segment", "Customer City", "Customer Country",
#                     "Customer State", "Customer Street", "Customer Zipcode"]
customer_dimension = create_dimension_tables(df_raw, extract_columns, customer_columns, "customer_dimension")
customer_dimension.show()

print('Creating Dimension table for Products')
# product_columns = ["Product Card Id", "Product Category Id", "Category Name", "Product Description",
#                    "Product Image", "Product Name", "Product Price", "Product Status"]
product_dimension = create_dimension_tables(df_raw, extract_columns, product_columns, "product_dimension")
product_dimension.show()

print('Creating Dimension table for Location')
# location_columns = ["Order Zipcode", "Order City", "Order State", "Order Region","Order Country",
#                     "Latitude", "Longitude"]
location_dimension = create_dimension_tables(df_raw, extract_columns, location_columns, "location_dimension")
location_dimension.show()

print('Creating Dimension table for Orders')
# order_columns = ["Order Id","Order date (DateOrders)", "Order Customer Id", "Order Item Id",
#                 "Order Item Discount", "Order Item Discount Rate", "Order Item Product Price",
#                 "Order Item Profit Ratio", "Order Item Quantity", "Sales per customer", "Sales",
#                 "Order Item Total", "Order Profit Per Order", "Order Status"]
order_dimension = create_dimension_tables(df_raw, extract_columns, order_columns, "order_dimension")
order_dimension.show()


print('Creating Dimension table for Shipping')
# shipping_columns = ["Shipping date (DateOrders)", "Days for shipping (real)", "Days for shipment (scheduled)",
#                     "Shipping Mode","Delivery Status"]
shipping_dimension = create_dimension_tables(df_raw, extract_columns, shipping_columns, "shipping_dimension")
shipping_dimension.show()

print('Creating Dimension table for Departments')
# department_columns = ["Department Id", "Department Name" ,"Market"]
department_dimension = create_dimension_tables(df_raw, extract_columns, department_columns, "department_dimension")
department_dimension.show()

fact_column = ["Type"]

print('Creating Dimension table for Metadata')
def extract_metadata(df, columns_to_extract):
    extracted_cols = [col("metadata").getItem(col_name).alias(col_name) for col_name in columns_to_extract]
    return df.select(*extracted_cols)
# metadata_columns = ["key","offset","partition","time","topic"]
metadata_dimension = create_dimension_tables(df_raw, extract_metadata, metadata_columns, "metadata_dimension")

# Define the output file path
output_path = gcs_bucket_path + "transformed_data/"

dataframes = {"customer_dimension":customer_dimension , "product_dimension":product_dimension , "location_dimension":location_dimension,
		    "order_dimension":order_dimension, "shipping_dimension":shipping_dimension ,"department_dimension":department_dimension,
             "metadata_dimension":metadata_dimension}

def write_to_gcs(dataframes,output_path):
    print('Starting to Export Raw Streaming data to  GCS...')
    for name,dataframe in dataframes.items():
        print(name)
        dataframe.write.mode("overwrite").option("header","true").option("compression","none").parquet(output_path+name+".parquet")
        print(f"Exported Dataframe {name} to GCS File Path {output_path}")

write_to_gcs(dataframes,output_path)
