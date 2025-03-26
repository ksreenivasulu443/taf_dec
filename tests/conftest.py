from pyspark.sql import SparkSession
import pytest
import os
import yaml


@pytest.fixture(scope='session')
def spark_session(request):
    # taf_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # snow_jar = taf_path +'/jars/snowflake-jdbc-3.14.3.jar'
    # postgres_jar = taf_path +'/jars/postgresql-42.2.5.jar'
    # azure_storage = taf_path +'/jars/azure-storage-8.6.6.jar'
    # hadoop_azure = taf_path +'/jars/hadoop-azure-3.3.1.jar'
    # sql_server = taf_path +'/jars/mssql-jdbc-12.2.0.jre8.jar'
    # jar_path = snow_jar + ',' + postgres_jar + ',' + azure_storage + ',' + hadoop_azure + ',' + sql_server
    spark = SparkSession.builder.master("local[2]") \
        .appName("pytest_framework") \
        .getOrCreate()
    return spark



@pytest.fixture(scope='module')
def read_config(request):
    config_path = request.node.fspath.dirname+'/config.yml'
    with open(config_path,'r') as f:
        config_data = yaml.safe_load(f)
    return config_data


@pytest.fixture(scope='module')
def read_data(spark_session,read_config, request):
    config_data = read_config
    print("config yml data", config_data)
    print("source config data", config_data['source'])
    print("target config data", config_data['target'])
    print("validation config data", config_data['validations'])
    print("source file path", config_data['source']['path'])
    print("source file type", config_data['source']['type'])
    print("target database table", config_data['target']['table'])
    print("taregt  type", config_data['target']['type'])













@pytest.fixture(scope='session')
def spark_session_cloud():
    spark = SparkSession.builder.appName('test auto').getOrCreate()
    yield spark
    spark.stop()










