from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data(*args, **kwargs):
    spark = (
        SparkSession
        .builder
        .appName('Test spark')
        .getOrCreate()
    )
    kwargs['context']['spark'] = spark
    ...
