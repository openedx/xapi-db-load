import asyncio

import urwid

from xapi_db_load.async_app import App

GO_TEXT = "Create Test Data"
LOAD_TEXT = "Load from Object Storage Only"
RUNNING_TEXT = "Running"


class LoadDisplay:
    def __init__(self, app):
        self.app = app
        self.widget = LoadData(self.app)

    def show(self):
        if self.widget is None:
            self.widget = LoadData(self.app)


class LoadData(urwid.WidgetWrap):
    def __init__(self, app):
        self.app = app
        self.title = urwid.BigText(
            ("banner", "Load Test Data"), urwid.HalfBlock5x4Font()
        )
        s = app.runner.backend.get_backend_summary()

        self.summary = urwid.Text(
            """{backend}
    - {num_xapi_batches} batches of {batch_size} events for {total_events:,} events
    - {num_actors} actors, with profiles saved {num_actor_profile_changes} times
    - {num_courses} courses, with {num_course_publishes} publishes
            """.format(**s)
        )
        self.go_button = urwid.Button(GO_TEXT, self.go_pressed)
        self.load_button = urwid.Button(LOAD_TEXT, self.load_pressed)
        self.all_widgets = [
            urwid.Padding(self.title, width="clip", align="center"),
            self.summary,
            urwid.Padding(self.go_button, width="clip", align="left"),
            urwid.Padding(self.load_button, width="clip", align="left"),
            urwid.Divider("-"),
        ]
        self.to_do_widgets = []

        for item in self.app.runner.test_data_tasks:
            self.to_do_widgets.append(
                urwid.ProgressBar(
                    "progress_empty",
                    "progress_full",
                    current=item.get_complete(),
                    done=1,
                    satt=None,
                )
            )
            self.all_widgets.append(urwid.Text(item.task_name))
            self.all_widgets.append(self.to_do_widgets[-1])

        self.overall_progress_label = urwid.Text("Overall Progress", align="center")
        self.overall_progress_bar = urwid.ProgressBar(
            "progress_empty",
            "progress_full",
            current=self.app.runner.get_overall_progress(),
            done=1,
            satt=None,
        )
        self.overall_time_label = urwid.Text("Overall Time", align="center")
        self.all_widgets.append(
            urwid.LineBox(
                urwid.Pile(
                    [
                        self.overall_progress_label,
                        self.overall_progress_bar,
                        self.overall_time_label,
                    ]
                )
            )
        )
        self.progress_pile = urwid.Pile(self.all_widgets)
        self.widget = urwid.LineBox(self.progress_pile)

        super().__init__(self.widget)

    def go_pressed(self, button):
        self.app.log("Go pressed")
        if self.app.runner.running:
            self.app.log("Already running")
            return

        self.app.runner.reset_status()

        self.load_button.set_label(RUNNING_TEXT)
        self.go_button.set_label(RUNNING_TEXT)
        asyncio.create_task(self.update_status())
        asyncio.create_task(self.app.runner.run())

    def load_pressed(self, button):
        # TODO: This can be combined with go_pressed using user data
        self.app.log("Load pressed")
        if self.app.runner.running:
            self.app.log("Already running")
            return

        self.app.runner.reset_status()

        self.load_button.set_label(RUNNING_TEXT)
        self.go_button.set_label(RUNNING_TEXT)
        asyncio.create_task(self.update_status())
        asyncio.create_task(self.app.runner.run(load_db_only=True))

    async def update_status(self):
        while True:
            assert len(self.to_do_widgets) == len(self.app.runner.test_data_tasks)
            for i in range(len(self.to_do_widgets)):
                item = self.to_do_widgets[i]
                item.set_completion(self.app.runner.test_data_tasks[i].get_complete())

            self.logger.info("Updating status")
            self.overall_progress_bar.set_completion(
                self.app.runner.get_overall_progress()
            )
            self.overall_time_label.set_text(
                f"Elapsed time: {self.app.runner.get_overall_time()}"
            )

            if self.app.runner.finished:
                self.go_button.set_label(GO_TEXT)
                self.load_button.set_label(LOAD_TEXT)
                self.app.draw_screen()
                break

            self.app.draw_screen()
            await asyncio.sleep(1)

    def keypress(self, size, key):
        if key == "up":
            App.get_shared_instance().ui.main_display.frame.focus_position = "header"

        return super(LoadData, self).keypress(size, key)
