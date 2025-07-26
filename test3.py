# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder \
#     .appName("MongoRead") \
#     .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
#     .config("spark.mongodb.read.connection.uri", "mongodb://localhost") \
#     .config("spark.mongodb.read.database", "testdb") \
#     .config("spark.mongodb.read.collection", "testcoll") \
#     .getOrCreate()
#
# df = spark.read.format("mongodb").load()
# df.show()

from pyspark.sql import SparkSession

# Initialize the Spark session with MongoDB connector
spark = SparkSession.builder \
    .appName("MongoDBIntegration") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()


df = spark.read.format("mongo").option("uri", "mongodb://127.0.0.1:27017/testdb.testcoll").load()
df.show(truncate=False)
print(df.count())
