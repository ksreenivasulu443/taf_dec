from pyspark.sql.functions import col, regexp_extract, udf
import datetime
from pyspark.sql.types import BooleanType
def name_check(target, column):
    pattern = "^[a-zA-Z]"
    # Add a new column 'is_valid' indicating if the name contains only alphabetic characters
    df = target.withColumn("is_valid", regexp_extract(col(column), pattern, 0) != "")
    df.show()
    target_count = target.count()
    failed = df.filter('is_valid = False ')
    failed.show()
    failed_count = failed.count()
    if failed_count > 0:
        status = 'FAIL'
    else:
        status ='PASS'

    return status



def check_range(target, column, min_val, max_val):
    invalid_count = target.filter((col(column) < min_val) | (col(column) > max_val)).count()
    return invalid_count == 0


def date_check(target, column):
    def is_valid_date_format(date_str: str) -> bool:
        try:
            # Try to parse the string in the format 'dd-mm-yyyy'
            datetime.strptime(date_str, "%d-%m-%Y")
            return True
        except ValueError:
            return False

    date_format_udf = udf(is_valid_date_format, BooleanType())

    df_with_validation = target.withColumn("is_valid_format", date_format_udf(col("column"))).filter(
        'is_valid_format = False')

    failed = df_with_validation.count()
    if failed > 0:
        status ='FAIL'
    else:
        status ='PASS'
    return status