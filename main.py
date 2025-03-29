from pyspark.sql import SparkSession
from src.utility.general_utility import flatten
# Initialize Spark session
spark = (SparkSession.builder.master("local[2]")
        .config("spark.jars", "/Users/admin/PycharmProjects/taf_dec/jars/postgresql-42.2.5.jar")
        .appName("pytest_framework")
        .getOrCreate())
print(spark.sparkContext.getConf().get("spark.jars"))

df = spark.read.json("/Users/admin/PycharmProjects/taf_dec/input_files/Complex2.json", multiLine=True)

df.show()

df2 = flatten(df)

