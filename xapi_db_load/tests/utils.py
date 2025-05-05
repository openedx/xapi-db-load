"""
Test utilities for xapi-db-load.
"""

import pathlib
from contextlib import contextmanager
from unittest.mock import patch

from xapi_db_load.main import get_config


@contextmanager
def override_config(config_path: str, tmppath: pathlib.Path):
    """
    Override the config file with runtime variables (temp file paths, etc).

    Overrides for both the test code and the loading code. `tmppath` is a pytest
    fixture that provides a temporary directory for the test to use and cleans it
    up after the test is done.
    """
    test_config = get_config(config_path)
    test_config["log_dir"] = str(tmppath)
    test_config["csv_output_destination"] = str(tmppath)

    with patch("xapi_db_load.main.get_config") as mock_config:
        mock_config.return_value = test_config
        try:
            yield test_config
        finally:
            pass
