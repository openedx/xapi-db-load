"""
Tests for xapi-db-load.py.
"""
import gzip
import os
from unittest.mock import patch

from click.testing import CliRunner

from xapi_db_load.main import load_db


def test_csv(tmpdir):
    test_path = tmpdir.mkdir("test_csv")
    runner = CliRunner()
    result = runner.invoke(
        load_db,
        "--backend=csv_file "
        "--num_batches=3 "
        "--batch_size=5 "
        f'--csv_output_directory="{test_path}"',
    )
    assert "Done! Added 15 rows!" in result.output
    assert "Total run time" in result.output

    with gzip.open(os.path.join(test_path, "xapi.csv.gz"), "r") as csv:
        assert len(csv.readlines()) == 15
    with gzip.open(os.path.join(test_path, "courses.csv.gz"), "r") as csv:
        assert len(csv.readlines()) >= 1
    with gzip.open(os.path.join(test_path, "blocks.csv.gz"), "r") as csv:
        assert len(csv.readlines()) > 1


@patch("xapi_db_load.main.ralph")
def test_ralph_clickhouse(mock_ralph):
    runner = CliRunner()
    result = runner.invoke(
        load_db,
        "--backend=ralph_clickhouse "
        "--num_batches=3 "
        "--batch_size=5 "
        "--db_host=fake "
        "--db_name=fake "
        "--db_username=fake "
        "--db_password=fake "
        "--lrs_url=fake "
        "--lrs_username=fake "
        "--lrs_password=fake",
        catch_exceptions=False,
    )
    print(mock_ralph.mock_calls)
    print(result.output)
    assert "Done! Added 15 rows!" in result.output
    assert "Total run time" in result.output
