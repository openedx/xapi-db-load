import urwid
from urwid import Widget

from xapi_db_load.async_app import App
from xapi_db_load.ui.config_ui import ConfigDisplay
from xapi_db_load.ui.load_ui import LoadDisplay
from xapi_db_load.ui.log_ui import LogDisplay


class SubDisplays:
    """
    Keeps track of which sub-displays exist and which is currently active.
    """

    def __init__(self, app):
        self.app = app
        self.load_display = LoadDisplay(self.app)
        self.config_display = ConfigDisplay(self.app)
        self.log_display = LogDisplay(self.app)
        self.active_display = self.load_display

    def active(self):
        return self.active_display


class MenuButton(urwid.Button):
    """
    Formatting for the top menu bar buttons.
    """

    button_left = urwid.Text("[")
    button_right = urwid.Text("]")


class MainFrame(urwid.Frame):
    """
    Frame for the main display, in charge of handling focus changes among the widgets.

    Wraps display of the menu bar (header), body, and footer.
    """

    def __init__(
        self, body: Widget, header: Widget, footer: Widget, delegate: "MainDisplay"
    ):
        self.delegate = delegate
        self.current_focus = None
        super().__init__(body, header, footer)

    def mouse_event(self, size, event, button, col, row, focus):
        current_focus = self.delegate.widget.get_focus_widgets()[-1]
        self.current_focus = current_focus
        return super(MainFrame, self).mouse_event(size, event, button, col, row, focus)


class MainDisplay:
    """
    Main body of the application, driving the tab-like interface.

    Uses the Menu to switch between the different sub-displays.
    """

    def __init__(self, ui, app):
        self.ui = ui
        self.app = app

        self.menu_display = MenuDisplay(self.app, self)
        self.sub_displays = SubDisplays(self.app)

        self.frame = MainFrame(
            self.sub_displays.active().widget,
            header=self.menu_display.widget,
            footer=urwid.AttrMap(urwid.Text(""), "shortcutbar"),
            delegate=self,
        )
        self.widget = self.frame

    def update_active_sub_display(self):
        """
        Set the display to the active widget.
        """
        self.frame.contents["body"] = (self.sub_displays.active().widget, None)

    def show_load(self, user_data):
        """
        Shows the data load sub-display.
        """
        self.sub_displays.active_display = self.sub_displays.load_display
        self.update_active_sub_display()

    def show_config(self, user_data):
        """
        Shows the data config sub-display.
        """
        self.sub_displays.active_display = self.sub_displays.config_display
        self.update_active_sub_display()

    def show_log(self, user_data):
        """
        Shows the data log tailer sub-display.
        """
        self.sub_displays.active_display = self.sub_displays.log_display
        self.sub_displays.log_display.show()
        self.update_active_sub_display()

    def request_redraw(self, extra_delay=0.0):
        self.app.ui.loop.set_alarm_in(0.25 + extra_delay, self.redraw_now)

    def redraw_now(self, sender=None, data=None):
        self.app.ui.loop.screen.clear()

    def quit(self, sender=None):
        """
        Quit the application.
        """
        raise urwid.ExitMainLoop


class MenuColumns(urwid.Columns):
    handler: MainDisplay

    def keypress(self, size, key):
        """
        Moves us from the menu bar to the body of the screen on keypress.
        """
        if key == "tab" or key == "down":
            self.handler.frame.focus_position = "body"

        return super(MenuColumns, self).keypress(size, key)


class MenuDisplay:
    """
    Our top menu bar.
    """

    def __init__(self, app: App, handler: MainDisplay):
        self.app = app

        button_load = (18, MenuButton("Load Test Data", on_press=handler.show_load))
        button_config = (10, MenuButton("Config", on_press=handler.show_config))
        button_log = (7, MenuButton("Log", on_press=handler.show_log))
        button_quit = (8, MenuButton("Quit", on_press=handler.quit))
        buttons = [
            button_load,
            button_config,
            button_log,
            button_quit,
        ]

        columns = MenuColumns(buttons, dividechars=1)
        columns.handler = handler
        self.widget = urwid.AttrMap(columns, "menubar")
