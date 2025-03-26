from pyspark.sql import SparkSession
import pytest
import os


@pytest.fixture(scope='session')
def spark_session(request):
    taf_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    snow_jar = taf_path +'/jars/snowflake-jdbc-3.14.3.jar'
    postgres_jar = taf_path +'/jars/postgresql-42.2.5.jar'
    azure_storage = taf_path +'/jars/azure-storage-8.6.6.jar'
    hadoop_azure = taf_path +'/jars/hadoop-azure-3.3.1.jar'
    sql_server = taf_path +'/jars/mssql-jdbc-12.2.0.jre8.jar'
    jar_path = snow_jar + ',' + postgres_jar + ',' + azure_storage + ',' + hadoop_azure + ',' + sql_server
    spark = SparkSession.builder.master("local[2]") \
        .appName("pytest_framework") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()
    print(taf_path,jar_path)
    yield spark
    spark.stop()













@pytest.fixture(scope='session')
def spark_session_cloud():
    spark = SparkSession.builder.appName('test auto').getOrCreate()
    yield spark
    spark.stop()










