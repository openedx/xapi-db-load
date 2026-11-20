"""
Tests for xapi-db-load.py.
"""

import gzip
import json
import os
import re
from contextlib import contextmanager
from csv import reader as csv_reader
from unittest.mock import (
    AsyncMock,
    MagicMock,
    patch,
)

import yaml
from click.testing import CliRunner
from clickhouse_connect.driver.exceptions import DatabaseError

from xapi_db_load.backends.base_async_backend import redact_credentials
from xapi_db_load.constants import (
    DEFAULT_NUM_CATALOG_PATHWAYS,
    DEFAULT_NUM_ENROLLMENTS_PER_PATHWAY,
)
from xapi_db_load.main import load_db


@contextmanager
def override_config(config_path, tmp_path, **overrides):
    """
    Override the config file with runtime variables (temp file paths, etc).

    Overrides for both the test code and the loading code.
    """
    with open(config_path, "r") as f:
        test_config = yaml.safe_load(f)

    test_config["log_dir"] = str(tmp_path)
    test_config["csv_output_destination"] = str(tmp_path)
    test_config.update(overrides)

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
            ("catalog_pathways", DEFAULT_NUM_CATALOG_PATHWAYS),
            (
                "pathway_enrollments",
                DEFAULT_NUM_CATALOG_PATHWAYS
                * min(DEFAULT_NUM_ENROLLMENTS_PER_PATHWAY, test_config["num_actors"]),
            ),
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


@patch(
    "xapi_db_load.backends.base_async_backend.clickhouse_connect",
    new_callable=AsyncMock,
)
@patch(
    "xapi_db_load.backends.vector.getLogger",
    new_callable=MagicMock,
)
def test_vector_backend(mock_get_logger, _, tmp_path):
    """
    Run a test through the Vector backend, currently this just checks that the
    output indicates success.
    """
    test_path = "xapi_db_load/tests/fixtures/small_vector_config.yaml"

    runner = CliRunner()

    with override_config(test_path, tmp_path):
        result = runner.invoke(
            load_db,
            f"--config_file {test_path}",
            catch_exceptions=False,
        )

    # This test should create 300 xAPI log statemetns
    assert mock_get_logger.return_value.info.call_count == 300

    last_logged_statement = mock_get_logger.return_value.info.call_args.args[0]

    # We check to make sure Vector's regex will parse what we're sending. We want it to match both
    # the LMS and our local logger formatter.
    # This is how things are generally formatted in the LMS
    test_str_1 = (
        "2026-02-24 20:26:13,006 INFO 42 [xapi_tracking] [user None] "
        f"[ip 172.19.0.1] logger.py:41 - {last_logged_statement}"
    )

    # This returns our message formatted with the abbreviated version we use for size and speed purposes
    formatter = mock_get_logger.return_value.addHandler.call_args.args[0].formatter
    test_str_2 = formatter._fmt.format(  # pylint: disable=protected-access
        name="xapi_tracking", message=last_logged_statement
    )

    # This is a direct copy and paste from Aspects' Vector common-post.toml
    msg_regex = r"^.* \[xapi_tracking\] [^{}]* (?P<tracking_message>\{.*\})$"

    # Quick test to make sure that what's being stored is at least parseable
    for s in (test_str_1, test_str_2):
        try:
            match = re.match(msg_regex, s)
            if match is not None and match.groups():
                statement = match.groups()[0]
                json.loads(statement)
            else:
                raise ValueError("No match found")
        except Exception as e:
            print(e)
            print("Exception! Regex testing: ")
            print(s)
            raise

    assert "Insert xAPI Events complete." in result.output
    assert "Insert Initial Enrollments complete." in result.output
    assert "ALL TASKS DONE!" in result.output
    assert "Run duration was" in result.output


@patch("xapi_db_load.backends.ralph.requests", new_callable=MagicMock)
@patch(
    "xapi_db_load.backends.base_async_backend.clickhouse_connect",
    new_callable=AsyncMock,
)
def test_ralph_backend(_mock_requests, _, tmp_path):
    """
    Run a test through the Ralph backend, currently this just checks that the
    output indicates success.
    """
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
def test_chdb_backend(_mock_ch, _mock_chdb, _mock_boto, tmp_path):
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


def _read_csv(test_config, prefix):
    """Return the rows of one gzipped CSV output file."""
    with gzip.open(
        os.path.join(test_config["log_dir"], f"{prefix}.csv.gz"), "rt"
    ) as csv_file:
        return list(csv_reader(csv_file))


def test_csv_backend_competency_load(tmp_path):
    """
    A competency load marks the taxonomy "competency" in its last column and
    writes both learner competency status files.
    """
    test_path = "xapi_db_load/tests/fixtures/small_csv_config.yaml"

    with override_config(test_path, tmp_path, taxonomy_type="competency") as test_config:
        result = CliRunner().invoke(
            load_db, f"--config_file {test_path}", catch_exceptions=False
        )

        assert "ALL TASKS DONE!" in result.output
        taxonomies = _read_csv(test_config, "taxonomies")
        assert taxonomies
        assert {row[-1] for row in taxonomies} == {"competency"}

        statuses = _read_csv(test_config, "student_competency_statuses")
        group_statuses = _read_csv(test_config, "student_competency_course_statuses")
        assert statuses
        assert group_statuses
        # id, actor_id, group_id, tag_id, taxonomy_id, course_key, status_id,
        # status_name, created, modified, dump_id, time_last_dumped
        assert {len(row) for row in group_statuses} == {12}


def test_csv_backend_tags_load(tmp_path):
    """A default load marks the taxonomy "tags" and writes no competency statuses."""
    test_path = "xapi_db_load/tests/fixtures/small_csv_config.yaml"

    with override_config(test_path, tmp_path) as test_config:
        CliRunner().invoke(load_db, f"--config_file {test_path}", catch_exceptions=False)

        assert {row[-1] for row in _read_csv(test_config, "taxonomies")} == {"tags"}
        # No files, so a load into an install without the competency tables
        # never tries them.
        for prefix in ("student_competency_statuses", "student_competency_course_statuses"):
            assert not os.path.exists(
                os.path.join(test_config["log_dir"], f"{prefix}.csv.gz")
            )


@patch(
    "xapi_db_load.backends.base_async_backend.clickhouse_connect",
    new_callable=AsyncMock,
)
def test_clickhouse_backend_competency_load(mock_clickhouse_connect, tmp_path):
    """A competency load inserts both status tables and a "competency" taxonomy."""
    test_path = "xapi_db_load/tests/fixtures/small_clickhouse_config.yaml"

    with override_config(test_path, tmp_path, taxonomy_type="competency"):
        result = CliRunner().invoke(
            load_db, f"--config_file {test_path}", catch_exceptions=False
        )

    assert "ALL TASKS DONE!" in result.output
    client = mock_clickhouse_connect.get_async_client.return_value
    inserts = {c.args[0]: c.args[1] for c in client.insert.call_args_list}
    assert inserts["student_competency_status"]
    assert inserts["student_competency_course_status"]
    assert {row[-1] for row in inserts["taxonomy"]} == {"competency"}


@patch(
    "xapi_db_load.backends.base_async_backend.clickhouse_connect",
    new_callable=AsyncMock,
)
@patch("xapi_db_load.backends.chdb.chdb")
@patch(
    "xapi_db_load.backends.chdb.boto3",
    new_callable=AsyncMock,
)
def test_chdb_backend_competency_load(_mock_boto, mock_chdb, _mock_ch, tmp_path):
    """A competency load writes both status tables through CHDB."""
    test_path = "xapi_db_load/tests/fixtures/small_chdb_config.yaml"

    with override_config(test_path, tmp_path, taxonomy_type="competency"):
        result = CliRunner().invoke(
            load_db, f"--config_file {test_path}", catch_exceptions=False
        )

    assert "ALL TASKS DONE!" in result.output
    sql = " ".join(str(c.args[0]) for c in mock_chdb.query.call_args_list)
    assert "student_competency_status" in sql
    assert "student_competency_course_status" in sql
    assert "'competency'" in sql


def test_redact_credentials():
    """The secret is hidden whole, the key all but its first 4 characters."""
    sql = "s3('file', 'AKIAEXAMPLEKEY', 'very-secret', 'NATIVE')"

    redacted = redact_credentials(sql, "AKIAEXAMPLEKEY", "very-secret")

    assert redacted == "s3('file', 'AKIA****', '****', 'NATIVE')"
    assert redact_credentials("key abc", "abc", None) == "key ****"
    assert redact_credentials("nothing", None, None) == "nothing"


@patch(
    "xapi_db_load.backends.base_async_backend.clickhouse_connect",
    new_callable=AsyncMock,
)
@patch("xapi_db_load.backends.chdb.chdb")
@patch(
    "xapi_db_load.backends.chdb.boto3",
    new_callable=AsyncMock,
)
def test_chdb_failed_load_fails_the_run_without_leaking_credentials(
    _mock_boto, _mock_chdb, mock_clickhouse_connect, tmp_path
):
    """
    A file that fails to load into ClickHouse fails the whole run at the end,
    and neither the logs nor the error show the S3 secret or the whole key.
    """
    test_path = "xapi_db_load/tests/fixtures/small_chdb_config.yaml"
    client = mock_clickhouse_connect.get_async_client.return_value
    client.command.side_effect = DatabaseError(
        "Code: 16. No such column taxonomy_type: s3('f', 'fake-key', 'fake-secret')"
    )

    with override_config(test_path, tmp_path):
        result = CliRunner().invoke(load_db, f"--config_file {test_path}")

    assert isinstance(result.exception, RuntimeError)
    assert "database load job(s) failed" in str(result.exception)
    assert "ALL TASKS DONE!" not in result.output
    assert "taxonomy" in result.output
    assert "fake****" in result.output
    assert "fake-secret" not in result.output
    assert "fake-key" not in result.output


@patch(
    "xapi_db_load.backends.base_async_backend.clickhouse_connect",
    new_callable=AsyncMock,
)
@patch("xapi_db_load.backends.chdb.chdb")
@patch(
    "xapi_db_load.backends.chdb.boto3",
    new_callable=AsyncMock,
)
def test_chdb_failed_write_does_not_leak_credentials(
    _mock_boto, mock_chdb, _mock_ch, tmp_path
):
    """A CHDB write that fails twice raises, naming the table, with the credentials redacted."""
    test_path = "xapi_db_load/tests/fixtures/small_chdb_config.yaml"
    mock_chdb.query.side_effect = Exception("Syntax error in s3('f', 'fake-key', 'fake-secret')")

    with override_config(test_path, tmp_path):
        result = CliRunner().invoke(load_db, f"--config_file {test_path}")

    assert isinstance(result.exception, RuntimeError)
    assert "failed twice" in str(result.exception)
    assert "fake-secret" not in str(result.exception)
    assert "fake-secret" not in result.output
    assert "fake-key" not in result.output
