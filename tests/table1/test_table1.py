import pytest
def count_val(source, target):
    source_cnt = source.count()
    target_cnt = target.count()


    if source_cnt == target_cnt:
        print("count is matching")
        return True
    else:
        print("count is not matching")
        return False
@pytest.mark.sanity
def test_count_check(read_data):
    source, target = read_data
    assert count_val(source, target)