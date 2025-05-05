import asyncio

import urwid

import xapi_db_load.ui.main_ui as Main
from xapi_db_load.async_app import App
from xapi_db_load.ui.themes import THEMES


class TextUI:
    """
    Top level urwid object, handles setting up the screen, palette, and the main loop.
    """

    config: dict
    app: App
    palette: list
    screen: urwid.raw_display.Screen
    main_display: Main.MainDisplay
    aio_loop: asyncio.AbstractEventLoop
    loop: urwid.MainLoop

    def __init__(self, config: dict, run_on_start: bool = True):
        urwid.set_encoding("UTF-8")
        self.config = config
        self.app = App(config, ui=self)
        self.palette = THEMES["default"]
        self.screen = urwid.raw_display.Screen()
        self.screen.register_palette(self.palette)
        self.main_display = Main.MainDisplay(self, self.app)
        self.aio_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.aio_loop)
        self.loop = urwid.MainLoop(
            self.main_display.widget,
            event_loop=urwid.AsyncioEventLoop(loop=self.aio_loop),
            screen=self.screen,
            handle_mouse=True,
        )
        self.app.set_main_loop(self.loop)

        # When testing we just want to load everything, not start the
        # whole loop.
        if run_on_start:
            self.loop.run()
