
from src.data_validations.count_check import count_val
def test_one(read_data,read_config):
    source, target = read_data
    read_config = read_config
    key_columns = read_config['validations']['count_check']['key_columns']
    source.show()
    target.show()

    status = count_val(source=source, target=target,key_columns=key_columns)
    assert status == 'PASS'



