import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, StructType
 
# create a Spark session for you by default.
spark = SparkSession.builder.appName('call-func').getOrCreate()

# to test if table exists in database or not - 
def check_if_table_exists(table_name, db_name):
	return spark.catalog.tableExists(f'{db_name}.{table_name}')