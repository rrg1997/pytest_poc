import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, StructType
 
# create a Spark session for you by default.
spark = SparkSession.builder.appName('pytest_poc').getOrCreate()

# content of test_sample.py
def inc(x):
    return x + 1


def test_answer():
    assert inc(3) == 4