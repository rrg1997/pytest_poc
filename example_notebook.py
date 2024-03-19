# Databricks notebook source
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType
spark = SparkSession.builder.appName("Pytest-POC").getOrCreate()

df=spark.read.format('delta').option('header','true').load('dbfs:/user/hive/warehouse/covid_data')
df.printSchema()

# COMMAND ----------

import pytest
from pyspark.sql import SparkSession

def test_table_existenece():
    spark = SparkSession.builder.getOrCreate()
    database_name = "default"
    table_name1 = "covid_data"
    tables = spark.catalog.listTables(database_name)
    table_exists = False

    for table in tables:
        if table.name == table_name1:
            table_exists = True
            break

    assert table_exists, F"INVALID TABLE: The '{table_name1}' table does not exist in the '{database_name}' database ..."
    print("Running Pytest Test Cases...")
pytest.main(["-v"])
