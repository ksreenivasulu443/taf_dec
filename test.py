from pyspark.sql import SparkSession

azure_storage = '/Users/admin/PycharmProjects/test_automation_project/jar/azure-storage-8.6.6.jar'
hadoop_azure = '/Users/admin/PycharmProjects/test_automation_project/jar/hadoop-azure-3.3.1.jar'
sql_server = '/Users/admin/PycharmProjects/taf/jars/mssql-jdbc-12.2.0.jre8.jar'
jar_path =  azure_storage + ',' + hadoop_azure + ',' + sql_server
spark = SparkSession.builder.master("local[*]") \
        .appName("pytest_framework") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()


adls_account_name = "decautoadls"  # Your ADLS account name
adls_container_name = "test"  # Your container name
key = "vnH8MP/h4VB5vbfsP1x9rAZ5PiyMkIk5RBPnxCbrAjupr7GXMiCv0fHDuySVA036WYaKQDVXcMzz+AStHfeBKQ=="  # Your Account Key

spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net", "SharedKey")
spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)

adls_path = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/raw/Contact_info_t.csv"

source_df = spark.read.csv(adls_path, inferSchema=True, header=True)

source_df.show()
#
#
#
df = spark.read.format("jdbc"). \
    option("url", "jdbc:sqlserver://decautoserver.database.windows.net:1433;database=decauto"). \
    option("user", 'decadmin'). \
    option("password", 'Dharmavaram1@'). \
    option("dbtable", '[dbo].[PeopleInfo]'). \
    option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver').load()

df.show()


