"""
Tests for loading a journeys dataset, with a fake ClickHouse client.
"""

import gzip
import json
import logging
import pathlib

import pytest
import yaml
from click.testing import CliRunner

from xapi_db_load.journeys import load as journeys_load
from xapi_db_load.journeys.generate import JourneyGenerator
from xapi_db_load.journeys.load import JourneyLoader
from xapi_db_load.main import load_journeys

CONFIG = pathlib.Path(__file__).parents[2] / "example_configs" / "journeys_oracle.yaml"
NOW = "2026-10-08 12:00:00"


class FakeClient:
    """Records inserts and commands; reports ``arrived`` events as present."""

    def __init__(self, arrived=None, missing=()):
        self.inserts = {}
        self.commands = []
        self.arrived = arrived
        self.missing = missing

    def query(self, sql):
        """Return the ``missing`` event ids."""
        assert "not in" in sql
        return type("Result", (), {"result_rows": [(m,) for m in self.missing]})

    def raw_insert(self, table, column_names=None, insert_block=None, fmt=None):
        self.inserts[table] = (column_names, insert_block, fmt)

    def command(self, sql):
        self.commands.append(sql)
        if sql.startswith("select count()"):
            return self.arrived
        return None


@pytest.fixture(name="dataset", scope="module")
def fixture_dataset(tmp_path_factory):
    """Generate the oracle dataset; return its dir, event row count and distinct event count."""
    out = tmp_path_factory.mktemp("journeys")
    conf = yaml.safe_load(CONFIG.read_text())["journeys"]
    conf["now"] = NOW
    JourneyGenerator(conf, "http://localhost:18000", logging.getLogger()).run(str(out))
    with gzip.open(out / "xapi.csv.gz", "rt") as f:
        lines = f.read().splitlines()
    unique_ids = {line.split(",", 1)[0] for line in lines}
    return out, len(lines), len(unique_ids)


def _loader(dataset, backend="clickhouse", arrived=None, **config):
    """Return a loader for the dataset with a fake client reporting ``arrived`` events."""
    out, _, n_unique = dataset
    client = FakeClient(n_unique if arrived is None else arrived)
    config = {"backend": backend, "lrs_url": "http://ralph/xAPI/statements/",
              "lrs_username": "u", "lrs_password": "p", **config}
    return JourneyLoader(config, str(out), logging.getLogger(), client=client), client


def test_clickhouse_backend_loads_everything(dataset):
    loader, client = _loader(dataset)
    summary = loader.run()
    assert set(client.inserts) == {
        "event_sink.course_overviews", "event_sink.course_blocks", "event_sink.external_id",
        "event_sink.user_profile", "journeys.expected_engagement",
        "journeys.expected_video_seconds", "journeys.event_ids", "xapi.xapi_events_all",
    }
    columns, block, fmt = client.inserts["xapi.xapi_events_all"]
    assert columns == ["event_id", "emission_time", "event"] and fmt == "CSV"
    assert block == gzip.open(dataset[0] / "xapi.csv.gz").read()
    assert client.inserts["journeys.expected_engagement"][2] == "CSVWithNames"
    assert len(client.inserts["journeys.event_ids"][1].decode().splitlines()) == dataset[2]
    assert f"Loaded {dataset[2]} xAPI events" in summary
    assert "system reload dictionaries" in client.commands


def test_database_names_come_from_config(dataset):
    loader, client = _loader(
        dataset, db_name="openedx", event_sink_db_name="sink", expected_db_name="answers"
    )
    loader.run()
    assert "openedx.xapi_events_all" in client.inserts
    assert "sink.course_blocks" in client.inserts
    assert "answers.expected_video_seconds" in client.inserts


def test_ralph_backend_posts_each_event_once(dataset, monkeypatch):
    posted = []

    class Response:
        def raise_for_status(self):
            pass

    def post(url, auth, json, timeout):  # pylint: disable=redefined-outer-name,unused-argument
        assert url == "http://ralph/xAPI/statements/" and auth == ("u", "p")
        assert len(json) <= journeys_load.RALPH_BATCH_SIZE
        posted.extend(json)
        return Response()

    monkeypatch.setattr(journeys_load.requests, "post", post)
    loader, client = _loader(dataset, backend="ralph")
    loader.run()
    assert "xapi.xapi_events_all" not in client.inserts
    _, n_rows, n_unique = dataset
    assert n_rows > n_unique, "the oracle dataset should include duplicate events"
    assert len(posted) == n_unique == len({s["id"] for s in posted})


def test_vector_backend_logs_every_event(dataset, capsys):
    loader, client = _loader(dataset, backend="vector")
    loader.run()
    lines = capsys.readouterr().out.splitlines()
    assert "xapi.xapi_events_all" not in client.inserts
    assert len(lines) == dataset[1]
    assert lines[0].startswith(" [xapi_tracking] [] {")
    json.loads(lines[0].split("[] ", 1)[1])


def test_wait_times_out_when_events_are_missing(dataset, caplog):
    loader, client = _loader(dataset, arrived=3)
    with gzip.open(dataset[0] / "xapi.csv.gz", "rt") as f:
        missing_id = f.readline().split(",", 1)[0]
    client.missing = [missing_id]
    with pytest.raises(TimeoutError, match=f"Only 3 of {dataset[2]}"):
        loader.wait(dataset[2], wait_seconds=0)
    assert f"Missing event {missing_id}" in caplog.text


def test_wait_polls_until_events_arrive(dataset, monkeypatch):
    loader, client = _loader(dataset, arrived=0)
    counts = iter([0, dataset[2]])
    monkeypatch.setattr(client, "command", lambda sql: next(counts))
    monkeypatch.setattr(journeys_load.time, "sleep", lambda s: None)
    loader.wait(dataset[2], wait_seconds=60)


def test_bad_backend(dataset):
    with pytest.raises(ValueError, match="backend must be one of"):
        _loader(dataset, backend="kafka")


def test_missing_dataset(tmp_path):
    with pytest.raises(ValueError, match="no manifest.json"):
        JourneyLoader({}, str(tmp_path), logging.getLogger(), client=FakeClient())


def test_cli(dataset, tmp_path, monkeypatch):
    out, _, n_unique = dataset
    monkeypatch.setattr(JourneyLoader, "_connect", lambda self: FakeClient(n_unique))
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.safe_dump({"backend": "clickhouse"}))
    result = CliRunner().invoke(
        load_journeys, ["--config_file", str(config_file), "--dataset_dir", str(out)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert f"Loaded {n_unique} xAPI events" in result.output

    result = CliRunner().invoke(load_journeys, ["--config_file", str(config_file)])
    assert result.exit_code == 2 and "Set --dataset_dir" in result.output

    result = CliRunner().invoke(
        load_journeys, ["--config_file", str(config_file), "--dataset_dir", str(tmp_path)]
    )
    assert result.exit_code == 2 and "no manifest.json" in result.output
