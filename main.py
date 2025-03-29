from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.master("local[2]") \
        .config("spark.jars", "/Users/admin/PycharmProjects/taf_dec/jars/postgresql-42.2.5.jar") \
        .config("spark.driver.extraClassPath", "/Users/admin/PycharmProjects/taf_dec/jars/postgresql-42.2.5.jar") \
        .config("spark.executor.extraClassPath", "/Users/admin/PycharmProjects/taf_dec/jars/postgresql-42.2.5.jar") \
        .appName("pytest_framework") \
        .getOrCreate()


# jdbc_url = "jdbc:postgresql://localhost:5432/postgres"  # Replace with actual values
#
# db_properties = {
#     "user": "postgres",    # Replace with your DB username
#     "password": "Dharmavaram1@",  # Replace with your DB password
#     "driver": "org.postgresql.Driver"
# }
#
#
# df = (spark.read
#     .format("jdbc")
#     .option("url", jdbc_url)
#     .option("dbtable", "employees")
#     .option("user", db_properties["user"])
#     .option("password", db_properties["password"])
#     .option("driver", db_properties["driver"])
#     .load())
#
# df.show()

import json

from pyspark.sql.types import StructType

def read_schema():
    schema_file_path  = "/Users/admin/PycharmProjects/taf_dec/tests/table1/schema.json"
    with open(schema_file_path,'r') as f:
        schema  = StructType.fromJson(json.load(f))
    return schema

print(read_schema())
