"""
Tests for xapi-db-load.py.
"""

import gzip
import os
from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
from click.testing import CliRunner

from xapi_db_load.main import load_db


@contextmanager
def override_config(config_path, tmp_path):
    """
    Override the config file with runtime variables (temp file paths, etc).

    Overrides for both the test code and the loading code.
    """
    with open(config_path, "r") as f:
        test_config = yaml.safe_load(f)

    test_config["log_dir"] = str(tmp_path)
    test_config["csv_output_destination"] = str(tmp_path)

    with patch("xapi_db_load.main.get_config") as mock_config:
        mock_config.return_value = test_config
        try:
            yield test_config
        finally:
            pass


def test_csv_backend(tmp_path):
    """
    Run a test through the CSV backend, and ensure the expected number of rows are
    created for each type of output file.
    """
    test_path = "xapi_db_load/tests/fixtures/small_csv_config.yaml"

    with override_config(test_path, tmp_path) as test_config:
        runner = CliRunner()
        result = runner.invoke(
            load_db, f"--config_file {test_path}", catch_exceptions=False
        )

        assert "Write CSV xAPI complete." in result.output
        assert "ALL TASKS DONE!" in result.output
        assert "Total duration:" in result.output

        makeup = test_config["course_size_makeup"]["small"]

        expected_enrollments = (
            test_config["num_course_sizes"]["small"] * makeup["actors"]
        )
        expected_statements = (
            test_config["num_xapi_batches"] * test_config["batch_size"]
            + expected_enrollments
        )
        expected_profiles = (
            test_config["num_actors"] * test_config["num_actor_profile_changes"]
        )
        expected_external_ids = test_config["num_actors"]
        expected_courses = (
            test_config["num_course_sizes"]["small"]
            * test_config["num_course_publishes"]
        )

        # We want all the configured block types, which are currently everything in
        # the config except the actor and forum post count
        expected_course_blocks = (
            sum(makeup.values()) - makeup["actors"] - makeup["forum_posts"]
        )

        # Plus 1 for the course block
        expected_blocks = (expected_course_blocks + 1) * expected_courses

        for prefix, expected in (
            ("xapi", expected_statements),
            ("courses", expected_courses),
            ("blocks", expected_blocks),
            ("external_ids", expected_external_ids),
            ("user_profiles", expected_profiles),
        ):
            with gzip.open(
                os.path.join(test_config["log_dir"], f"{prefix}.csv.gz"), "r"
            ) as csv:
                assert len(csv.readlines()) == expected, (
                    f"Bad row count in csv file {prefix}.csv.gz."
                )


@patch(
    "xapi_db_load.backends.base_async_backend.clickhouse_connect",
    new_callable=AsyncMock,
)
def test_clickhouse_backend(_, tmp_path):
    """
    Run a test through the ClickHouse backend, currently this just checks that the
    output indicates success.
    """
    test_path = "xapi_db_load/tests/fixtures/small_clickhouse_config.yaml"

    with override_config(test_path, tmp_path):
        runner = CliRunner()
        result = runner.invoke(
            load_db,
            f"--config_file {test_path}",
            catch_exceptions=False,
        )

    # TODO: Check the ClickHouse insert calls to ensure they are correct.

    assert "Insert xAPI Events complete." in result.output
    assert "Insert Initial Enrollments complete." in result.output
    assert "ALL TASKS DONE!" in result.output
    assert "Run duration was" in result.output


@patch("xapi_db_load.backends.ralph.requests", new_callable=AsyncMock)
@patch(
    "xapi_db_load.backends.base_async_backend.clickhouse_connect",
    new_callable=AsyncMock,
)
def test_ralph_backend(mock_requests, _, tmp_path):
    """
    Run a test through the Ralph backend, currently this just checks that the
    output indicates success.
    """
    mock_requests.post = MagicMock()
    test_path = "xapi_db_load/tests/fixtures/small_ralph_config.yaml"
    runner = CliRunner()

    with override_config(test_path, tmp_path):
        result = runner.invoke(
            load_db,
            f"--config_file {test_path}",
            catch_exceptions=False,
        )

    # TODO: Check the Ralph calls to ensure they are correct.

    assert "Insert xAPI Events complete." in result.output
    assert "Insert Initial Enrollments complete." in result.output
    assert "ALL TASKS DONE!" in result.output
    assert "Run duration was" in result.output


@patch(
    "xapi_db_load.backends.base_async_backend.clickhouse_connect",
    new_callable=AsyncMock,
)
@patch("xapi_db_load.backends.chdb.chdb")
@patch(
    "xapi_db_load.backends.chdb.boto3",
    new_callable=AsyncMock,
)
def test_chdb_backend(mock_ch, mock_chdb, mock_boto, tmp_path):
    """
    Run a test through the CHDB backend, currently this just checks that the
    output indicates success.
    """
    test_path = "xapi_db_load/tests/fixtures/small_chdb_config.yaml"

    with override_config(test_path, tmp_path):
        runner = CliRunner()
        result = runner.invoke(
            load_db,
            f"--config_file {test_path}",
            catch_exceptions=False,
        )

    # TODO: Check the CHDB insert calls to ensure they are correct.
    # TODO: Check the ClickHouse insert calls to ensure they are correct.

    assert "Insert xAPI Events complete." in result.output
    assert "Insert Initial Enrollments complete." in result.output
    assert "ALL TASKS DONE!" in result.output
    assert "Run duration was" in result.output
