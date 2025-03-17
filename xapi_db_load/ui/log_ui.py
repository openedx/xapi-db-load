import asyncio
from collections import deque

import urwid


class LogDisplay:
    def __init__(self, app):
        self.app = app
        self.widget = None

    @property
    def log_term(self):
        return self.widget

    def show(self):
        if self.widget is None:
            self.widget = LogTail(self.app)


class LogTail(urwid.WidgetWrap):
    def __init__(self, app):
        self.app = app
        self.log_tail = urwid.Text("Loading...")
        self.log = urwid.Scrollable(self.log_tail)
        # Start scrolled to the bottom
        self.log.set_scrollpos(-1)
        self.log_scrollbar = urwid.ScrollBar(self.log)
        asyncio.create_task(self.update())
        super().__init__(self.log_scrollbar)

    async def update(self):
        """
        Follow the log file and update the display with the last 200 lines.
        """
        with open(self.app.logfile_path, "r") as f:
            while True:
                # Return to the top, otherwise dequeue will not work
                f.seek(0)
                lines = deque(f, 200)
                self.log_tail.set_text("".join(lines))

                await asyncio.sleep(2)
