"""
Tests for ``xapi_db_load.main.get_config``: env-var overrides and defaults.
"""

import textwrap

import pytest

from xapi_db_load.constants import DEFAULT_LMS_URL
from xapi_db_load.main import get_config

# (env var name, config key it should populate)
ENV_VAR_OVERRIDES = [
    ("XAPI_DB_LOAD_CLICKHOUSE_PASSWORD", "db_password"),
    ("XAPI_DB_LOAD_AWS_SECRET_ACCESS_KEY", "s3_secret"),
    ("XAPI_DB_LOAD_RALPH_PASSWORD", "lrs_password"),
]


@pytest.fixture
def config_file(tmp_path):
    """Write a minimal YAML config to a temp file and return its path."""
    path = tmp_path / "config.yaml"
    path.write_text(
        textwrap.dedent(
            """
            backend: csv
            db_password: from_file
            s3_secret: from_file
            lrs_password: from_file
            """
        ).strip()
    )
    return str(path)


@pytest.mark.parametrize(
    "env_var,config_key",
    ENV_VAR_OVERRIDES,
    ids=[ck for _, ck in ENV_VAR_OVERRIDES],
)
def test_env_var_overrides_config_value(monkeypatch, config_file, env_var, config_key):
    """When the env var is set, it takes precedence over the YAML value."""
    monkeypatch.setenv(env_var, "from_env")
    conf = get_config(config_file)
    assert conf[config_key] == "from_env"


@pytest.mark.parametrize(
    "env_var,config_key",
    ENV_VAR_OVERRIDES,
    ids=[ck for _, ck in ENV_VAR_OVERRIDES],
)
def test_unset_env_var_falls_back_to_file(
    monkeypatch, config_file, env_var, config_key
):
    """When the env var is unset, the YAML value is kept."""
    monkeypatch.delenv(env_var, raising=False)
    conf = get_config(config_file)
    assert conf[config_key] == "from_file"


def test_lms_url_default_applied(config_file):
    """If ``lms_url`` is absent from the config, the default is filled in."""
    conf = get_config(config_file)
    assert conf["lms_url"] == DEFAULT_LMS_URL


def test_lms_url_from_config_is_preserved(tmp_path):
    """An explicit ``lms_url`` in the file overrides the default."""
    path = tmp_path / "config.yaml"
    path.write_text("backend: csv\nlms_url: https://my.lms.example.com\n")

    conf = get_config(str(path))
    assert conf["lms_url"] == "https://my.lms.example.com"


def test_config_file_path_is_recorded(config_file):
    """``get_config`` stamps the source path into the returned dict."""
    conf = get_config(config_file)
    assert conf["config_file"] == config_file
