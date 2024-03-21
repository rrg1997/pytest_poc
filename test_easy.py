import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from functions import *
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType

database_name = "default"
table_name1 = "covid_data"

# create a Spark session for you by default.
spark = SparkSession.builder.appName("Pytest-pyspark").getOrCreate()

def test_table_existenece():
    assert check_if_table_exists(table_name1, database_name) is True, F"INVALID TABLE: The '{table_name1}' table does not exist in the '{database_name}' database ..."