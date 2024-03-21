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

def test_table_exists(spark, default, covid_data):
    """Tests if a table exists in the specified database

    Args:
        spark: A SparkSession object
        database_name: The name of the database to check
        table_name: The name of the table to check
    """

    try:
        # Attempt to access the table metadata using catalog API
        spark.catalog.getTable(database_name, table_name)
        
        # Table exists
        assert True, f"Table '{table_name}' found in database '{database_name}'"

    except AnalysisException as e:
        # Check if the error message indicates table not found
        if "Table or view not found" in str(e):
            # Table does not exist
            assert False, f"Table '{table_name}' not found in database '{database_name}'"
        
        # Another error occurred
        raise e