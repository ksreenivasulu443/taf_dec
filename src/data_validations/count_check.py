from src.data_validations.records_only_target import records_only_in_target
from src.data_validations.records_only_in_source import records_only_in_source
from src.utility.report_lib import write_output
def count_val(source, target, key_columns):
    source_cnt = source.count()
    target_cnt = target.count()

    if source_cnt == target_cnt:
        status = 'PASS'
        write_output(validation_type="count check",
                     status=status,
                     details="Count is matching between source and target")

    else:
        print("count is not matching", abs(source_cnt-target_cnt))
        status = 'FAIL'
        records_only_in_target(source_df =source, target_df=target, key_columns=key_columns)
        records_only_in_source(source_df = source, target_df=target, key_columns=key_columns)

    return status