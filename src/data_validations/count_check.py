def count_val(source, target):
    source_cnt = source.count()
    target_cnt = target.count()

    if source_cnt == target_cnt:
        print("count is matching")
        satus = 'PASS'
    else:
        print("count is not matching")
        status = 'FAIL'
    return status