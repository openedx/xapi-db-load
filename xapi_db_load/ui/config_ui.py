"""
Urwid components for the configuration display screen.
"""

import urwid

from xapi_db_load.async_app import App


class ConfigDisplay:
    def __init__(self, app: App):
        self.app = app
        self.widget = ConfigWidget(self.app)

    @property
    def config_widget(self):
        return self.widget

    def show(self):
        if self.widget is None:
            self.widget = ConfigWidget(self.app)


class ConfigWidget(urwid.WidgetWrap):
    def __init__(self, app: App):
        self.app = app
        self.config_text = urwid.Text(self._get_config_contents())
        self.config = urwid.Scrollable(self.config_text)
        self.config_scrollbar = urwid.ScrollBar(self.config)
        self.config.set_scrollpos(0)

        super().__init__(self.config_scrollbar)

    def _get_config_contents(self):
        with open(self.app.config["config_file"], "r") as f:
            return f.read()
