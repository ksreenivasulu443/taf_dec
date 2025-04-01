def records_only_in_source(source_df, target_df, key_columns):
    """Validate records present only in the source."""
    only_in_source = source_df.select(key_columns).exceptAll(target_df.select(key_columns))

    count_only_in_source = only_in_source.count()
    if count_only_in_source > 0:
        failed_records = only_in_source.limit(5).collect()  # Get the first 5 failing rows
        failed_preview = [row.asDict() for row in failed_records]  # Convert rows to a dictionary for display
        status = "FAIL"
        return False
    else:
        status = "PASS"
        return True