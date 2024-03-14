import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, StructType

# content of test_sample.py
def inc(x):
    return x + 1


def test_answer():
    assert inc(3) == 4