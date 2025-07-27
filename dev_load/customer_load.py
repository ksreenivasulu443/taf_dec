from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
taf_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print("taf_path", taf_path)
azure_storage = os.path.join(taf_path, "jars", "azure-storage-8.6.6.jar")
hadoop_azure = os.path.join(taf_path, "jars", "hadoop-azure-3.3.1.jar")
sql_server = os.path.join(taf_path, "jars", "mssql-jdbc-12.2.0.jre8.jar")
jar_path = azure_storage + ',' + hadoop_azure + ',' + sql_server

print(azure_storage)
# Initialize Spark session
spark = SparkSession.builder.master("local[1]") \
        .appName("pytest_framework") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()

# ADLS Config
adls_account_name = "decautoadls"
adls_container_name = "test"
adls_key = "v3ZV7XHMx1CCSuHdxkuvr2iTCeZJKlWN9UchnRpLx4fBaVVKVlTUxbPFnL2ScuJBaA7kgycIRS8C+AStK0KemQ=="

spark.conf.set("fs.azure.account.key.decautoadls.dfs.core.windows.net", adls_key)

# Define ADLS path
adls_path = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/raw/customer/"

# Define schema explicitly (example)


# Read CSV file from ADLS
df = spark.read.format("csv") \
    .option("header", "true") \
    .load(adls_path)

df.show()

# JDBC Config
jdbc_url = "jdbc:sqlserver://decautoserver.database.windows.net:1433;database=decauto"
jdbc_properties = {
    "user": "decadmin",
    "password": "Dharmavaram1@",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Write DataFrame to Azure SQL table
df.write.jdbc(url=jdbc_url, table='customers_raw', mode='overwrite', properties=jdbc_properties)
