from src.utility.report_lib import write_output

from pyspark.sql import  SparkSession

spark = SparkSession.builder.master('local[1]').appName('test').getOrCreate()

def duplicate_check(df, key_col):
    """Validate that there are no duplicate rows in the specified columns."""
    duplicates = df.groupBy(key_col).count().filter("count > 1")
    print("duplcates dataframe")
    duplicates.show()
    duplicate_count = duplicates.count()
    print("duplcates count", duplicate_count)

    if duplicate_count > 0:
        failed_records = duplicates.limit(5).collect()  # Get the first 5 failing rows
        failed_preview = [row.asDict() for row in failed_records]  # Convert rows to a dictionary for display
        status = "FAIL"
        write_output(
            "Duplicate Check",
            status,
            f"Duplicate Count: {duplicate_count}, Sample Failed Records: {failed_preview}"
        )
        return status
    else:
        status = "PASS"
        write_output("Duplicate Check", status, "No duplicates found.")
        return status


target = spark.read.csv("/Users/admin/PycharmProjects/taf_dec/input_files/Contact_info_t.csv", header=True, inferSchema=True)

duplicate_check(target,['Identifier'])