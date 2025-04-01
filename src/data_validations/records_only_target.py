
def records_only_in_target(source_df, target_df, key_columns):
    """Validate records present only in the target."""
    only_in_target = target_df.select(key_columns).exceptAll(source_df.select(key_columns))


    count_only_in_target = only_in_target.count()
    if count_only_in_target > 0:
        failed_records = only_in_target.limit(5).collect()  # Get the first 5 failing rows
        failed_preview = [row.asDict() for row in failed_records]  # Convert rows to a dictionary for display
        status = "FAIL"
    else:
        status = "PASS"
        return True
