from src.data_validations.records_only_target import records_only_in_target
from src.data_validations.records_only_in_source import records_only_in_source
def count_val(source, target, key_columns):
    source_cnt = source.count()
    target_cnt = target.count()

    if source_cnt == target_cnt:
        print("count is matching")
        satus = 'PASS'
    else:
        print("count is not matching", abs(source_cnt-target_cnt))
        status = 'FAIL'
        records_only_in_target(source_df =source, target_df=target, key_columns=key_columns)
        records_only_in_source(source_df = source, target_df=target, key_columns=key_columns)

    return status