import asyncio
import os

import pytest
import urwid

from xapi_db_load.tests.utils import override_config
from xapi_db_load.ui.text_ui import TextUI


@pytest.fixture
def app(tmpdir):
    """
    Pytest fixture to create an urwid UI app instance for testing.
    """
    with override_config(
        "xapi_db_load/tests/fixtures/small_clickhouse_config.yaml", tmpdir
    ) as config:
        app = TextUI(config, run_on_start=False)
        return app


async def _quit_after(app_to_quit, secs=1):
    """
    Force the UI to quit after a given number of seconds.
    """
    await asyncio.sleep(secs)

    try:
        app_to_quit.main_display.quit()
    except urwid.event_loop.abstract_loop.ExitMainLoop:
        pass


@pytest.mark.asyncio
async def test_text_ui(app):
    """
    Simply start the app and quit it after a second.

    This loads the entire UI and uncovers any Python level issues.
    """
    try:
        assert app
        asyncio.create_task(_quit_after(app))
    finally:
        # Clean up the terminal settings when we exit, otherwise there will be artifacts
        # such as mouse handling being broken.
        os.system("stty ixon")
