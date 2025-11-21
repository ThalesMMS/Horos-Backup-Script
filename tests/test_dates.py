from horos_backup.dates import fmt_date_for_name, month_dir_for, parse_timestamp_to_parts


def test_parse_timestamp_variants():
    assert parse_timestamp_to_parts(60 * 60 * 24) == ("2001", "01", "02")
    assert parse_timestamp_to_parts("2023-02-03") == ("2023", "02", "03")
    assert parse_timestamp_to_parts("2023/02") == ("2023", "02", "01")
    assert parse_timestamp_to_parts("") == (None, None, None)


def test_fmt_date_for_name_and_month_dir(tmp_path, temp_config):
    assert fmt_date_for_name("2023-02-03") == "2023-02-03"
    assert fmt_date_for_name("2023/02") == "2023-02-01"
    assert fmt_date_for_name(None, fallback="X") == "X"

    month_dir = month_dir_for("2023-02-03", temp_config)
    assert month_dir.name == "2023_02"
    assert month_dir.parent == temp_config.paths.backup_root
