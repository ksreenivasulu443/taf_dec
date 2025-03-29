from pyspark.shell import spark
from pyspark.sql import SparkSession
import pytest
import os
import yaml
import json
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType


@pytest.fixture(scope='session')
def spark_session(request):
    taf_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    postgres_jar = taf_path +'/jars/postgresql-42.2.5.jar'
    jar_path =  postgres_jar
    print('#'*100)
    print("jarpath", jar_path)
    print('#' * 100)
    spark = (SparkSession.builder.master("local[2]")
        .config("spark.jars", "/Users/admin/PycharmProjects/taf_dec/jars/postgresql-42.2.5.jar")
        .config("spark.driver.extraClassPath", "/Users/admin/PycharmProjects/taf_dec/jars/postgresql-42.2.5.jar")
        .config("spark.executor.extraClassPath", "/Users/admin/PycharmProjects/taf_dec/jars/postgresql-42.2.5.jar")
        .appName("pytest_framework")
        .getOrCreate())
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

def read_cred(dirpath, env ='qa'):
    cred_file_path =  os.path.dirname(dirpath) + '/project_cred.yml'
    with open(cred_file_path,'r') as f:
        creds = yaml.safe_load(f)[env]
    return creds

def read_schema(dirpath):
    schema_file_path  =  dirpath + '/schema.json'
    with open(schema_file_path,'r') as f:
        schema  = StructType.fromJson(json.load(f))
    return schema


def flatten(df):
    # compute Complex Fields (Lists and Structs) in Schema
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    print("complex fields", complex_fields)
    print("length of complex fileds",len(complex_fields))
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))
        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if type(complex_fields[col_name]) == StructType:
            expanded = [col(col_name + '.' + k).alias( k) for k in
                        [n.name for n in complex_fields[col_name]]]
            print("expanded columns", expanded)
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))
            print("after explode o/p of df")
            #df.show()
    return df


def read_db(spark,config_data,dirpath):
    creds = read_cred(dirpath)[config_data['cred_lookup']]
    print("#"*100, end= '\n')
    print("creds ", creds)
    print("#"*100, end= '\n')
    if config_data['transformation'][0].lower() == 'y' and config_data['transformation'][1].lower() == 'sql':

        sql_query = read_query(dirpath)
        print("#" * 100, end='\n')
        print("sql query", sql_query)
        print("#" * 100, end='\n')
        df = (spark.read.format("jdbc").
            option("url", "jdbc:postgresql://localhost:5432/postgres").
            option("user", 'postgres').
            option("password", 'Dharmavaram1@').
            option("query", sql_query).
            option("driver", 'org.postgresql.Driver').load())

    else:
        df = spark.read.format("jdbc"). \
            option("url", creds['url']). \
            option("user", creds['user']). \
            option("password", creds['password']). \
            option("dbtable", config_data['table']). \
            option("driver", creds['driver']).load()
    return df

def read_file(spark, config_data, dirpath):
    type = config_data['type'].lower()
    if  type == 'csv':
        if config_data['schema'].lower() == 'y':
            schema = read_schema(dirpath)
            df = spark.read.csv(config_data['path'],
                                sep=config_data['options']['delimiter'],
                                header=config_data['options']['header'],
                                schema=schema)

        else:
            df = spark.read.csv(path=config_data['path'],
                                sep=config_data['options']['delimiter'] ,
                                header=config_data['options']['header'] ,
                                inferSchema=True)

    elif type == 'json':
        df = spark.read.json(config_data['path'], multiLine=True )
        df = flatten(df)
    elif type == 'parquet':
        df = spark.read.parquet(config_data['path'])
    elif type == 'avro':
        df = spark.read.format('avro').load(config_data['path'])
    elif type == 'txt':
        pass
    return df


@pytest.fixture(scope='module')
def read_data(spark_session,read_config, request):
    spark = spark_session
    config_data = read_config
    dirpath = request.node.fspath.dirname

    source_config = config_data['source']
    target_config = config_data['target']
    validation_config = config_data['validations']

    if source_config['type'] == 'database':
        source = read_db(spark=spark, config_data=source_config,dirpath=dirpath)
    else:
        source = read_file(spark=spark, config_data=source_config, dirpath=dirpath)

    if target_config['type'] == 'database':
        target = read_db(spark=spark, config_data=target_config,dirpath=dirpath)
    else:
        target = read_file(spark=spark, config_data= target_config, dirpath=dirpath)

    return source.drop(*source_config['exclude_cols']), target.drop(*target_config['exclude_cols'])













@pytest.fixture(scope='session')
def spark_session_cloud():
    spark = SparkSession.builder.appName('test auto').getOrCreate()
    yield spark
    spark.stop()










