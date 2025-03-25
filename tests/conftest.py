from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope='session')
def spark_session(request):
    snow_jar = '/Users/admin/PycharmProjects/taf_dec/jars/snowflake-jdbc-3.14.3.jar'
    postgres_jar = '/Users/admin/PycharmProjects/test_automation_project/jar/postgresql-42.2.5.jar'
    azure_storage = '/Users/admin/PycharmProjects/test_automation_project/jar/azure-storage-8.6.6.jar'
    hadoop_azure = '/Users/admin/PycharmProjects/test_automation_project/jar/hadoop-azure-3.3.1.jar'
    sql_server = '/Users/admin/PycharmProjects/taf/jars/mssql-jdbc-12.2.0.jre8.jar'
    jar_path = snow_jar + ',' + postgres_jar + ',' + azure_storage + ',' + hadoop_azure + ',' + sql_server
    spark = SparkSession.builder.master("local[2]") \
        .appName("pytest_framework") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()
    yield spark
    spark.stop()



