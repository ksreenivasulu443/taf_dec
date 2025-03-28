from pyspark.shell import spark
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

def read_query(dirpath):
    sql_query_path = dirpath + '/transformation.sql'
    with open(sql_query_path, "r") as file:
        sql_query = file.read()
    return sql_query

def read_db(spark,config_data,dirpath):
    if config_data['transformation'][0].lower() == 'y' and config_data['transformation'][1].lower() == 'sql':
        sql_query = read_query(dirpath)
        print("sql_query", sql_query)
        df = spark.read.format("jdbc"). \
            option("url", creds['url']). \
            option("user", creds['user']). \
            option("password", creds['password']). \
            option("query", sql_query). \
            option("driver", creds['driver']).load()

    else:
        df = spark.read.format("jdbc"). \
            option("url", creds['url']). \
            option("user", creds['user']). \
            option("password", creds['password']). \
            option("dbtable", config_data['table']). \
            option("driver", creds['driver']).load()
    return df

def read_file():
    pass


@pytest.fixture(scope='module')
def read_data(spark_session,read_config, request):
    config_data = read_config
    spark = spark_session
    dirpath = request.node.fspath.dirname

    source_config = config_data['source']
    target_config = config_data['target']
    validation_config = config_data['validations']

    if source_config['type'] == 'databse':
        source = read_db(spark, source_config,dirpath)
    else:
        source = read_file()

    if target_config['type'] == 'database':
        target = read_db(spark, target_config)
    else:
        target = read_file()













@pytest.fixture(scope='session')
def spark_session_cloud():
    spark = SparkSession.builder.appName('test auto').getOrCreate()
    yield spark
    spark.stop()










