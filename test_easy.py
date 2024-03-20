import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType

# create a Spark session for you by default.
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("pytest-pyspark") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

# Replace 'your_table_name' with the actual table name
table_name = "covid_data"
database_name = "default"  # Assuming Databricks default database

def test_table_existenece(spark):
    assert check_if_table_exists(table_name, database_name) is True, F"INVALID TABLE: The '{table_name}' table does not exist in the '{database_name}' database ..."





