
from src.data_validations.count_check import count_val
from src.data_validations.duplicate_validation import duplicate_check
def test_count_check(read_data,read_config):
    source, target = read_data
    read_config = read_config
    key_columns = read_config['validations']['count_check']['key_columns']
    status = count_val(source=source, target=target,key_columns=key_columns)
    assert status == 'PASS'

#
# def test_target_duplicate_check(read_data,read_config):
#     source, target = read_data
#     read_config = read_config
#     key_columns = read_config['validations']['duplicate_check']['key_columns']
#     status = duplicate_check( df=target,key_col=key_columns)
#     assert status == 'PASS'
#
# def test_source_duplicate_check(read_data, read_config):
#     source, target = read_data
#     read_config = read_config
#     key_columns = read_config['validations']['duplicate_check']['key_columns']
#     status = duplicate_check(df=source, key_col=key_columns)
#     assert status == 'PASS'



