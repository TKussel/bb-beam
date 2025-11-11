"""
Fixed CSS properties for Textual (replacing unsupported `column-gap` and `row-gap`).
Textual App Skeleton with Two Tabs: (a) Tasks and (b) Results
"""
from __future__ import annotations

import asyncio
import json
import os
from typing import Any, List

from dotenv import load_dotenv
from pydantic.types import UUID4
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.widget import Widget
from beam import BeamClient, BeamTask
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    ListItem,
    ListView,
    Select,
    TabbedContent,
    TabPane,
    TextArea,
    Static,
    Pretty
)

load_dotenv()

BEAM_PROXY_URL = os.getenv("BEAM_PROXY_URL", "http://localhost:8081")
BEAM_APIKEY = os.getenv("BEAM_APIKEY")
PROXY_ID = os.getenv("PROXY_ID")
APP_ID = f"bb-beam.{PROXY_ID}"
RESULTS_ENDPOINT = os.getenv("RESULTS_ENDPOINT", "https://httpbin.org/json")
BEAM_CLIENT = BeamClient(APP_ID, BEAM_APIKEY, BEAM_PROXY_URL)

__log_file = open("bb-beam.log", "a+")
def debug(*args, **kwargs):
    print(*args, **kwargs, file=__log_file)
    __log_file.flush()

class SectionTitle(Static):
    def __init__(self, text: str) -> None:
        super().__init__(text)

class NonFocusableVertical(Vertical):
    can_focus=False

class TaskLabel(Label):
    def __init__(self, task_id: UUID4, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_id = task_id


class TasksTab(TabPane):
    BINDINGS = [Binding("ctrl+enter", "submit_task", "Submit Task")]

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Vertical(
                SectionTitle("Sent Tasks"),
                ListView(*[ListItem(TaskLabel(v.id, k)) for (k, v) in self.app.tasks.items()], id="tasks_list"),
                id="tasks_upper_left",
            ),
            NonFocusableVertical(
                SectionTitle("Preview / Details"),
                Pretty("Select a task from the list to show it and its results here.", id="tasks_preview"),
                id="tasks_upper_right",
            ),
            id="tasks_upper",
        )
        yield Vertical(
            SectionTitle("Create Task"),
            Vertical(
                Horizontal(Label("to"), Input(placeholder="app.proxy.broker.beam-workshop.de, app.proxy2.broker.beam-workshop.de", id="task_to")),
                Horizontal(Label("metadata"), Input(placeholder='{"test": ["testmeta1", "testmeta2"]}', id="task_metadata")),
                Horizontal(Label("ttl"), Input(placeholder="e.g. 30s", id="task_ttl")),
                Horizontal(Label("body"), TextArea(placeholder="task body", id="task_body")),
            ),
            Button("Submit Task", id="task_submit", variant="primary"),
            id="tasks_lower",
        )

    @on(ListView.Highlighted, "#tasks_list")
    def _on_task_list_highlight(self, event: ListView.Highlighted) -> None:
        label = event.item.query_one(TaskLabel)
        task: BeamTask = self.app.tasks.get(label.task_id, "Gone")
        self.query_one("#tasks_preview", Pretty).update(task)

    @on(Button.Pressed, "#task_submit")
    async def _on_task_submit(self) -> None:
        task = self._collect_form()
        try:
            await BEAM_CLIENT.post_beam_task(task)
        except Exception as e:
            self.query_one("#tasks_preview", Pretty).update(f"Error: {e}")
        else:
            self.app.tasks[task.id] = task
        tasks_list = self.query_one("#tasks_list", ListView)
        tasks_list.append(ListItem(TaskLabel(task.id, f"Sent: {task.id}")))

    def _collect_form(self) -> BeamTask:
        to = self.query_one("#task_to", Input).value.strip()
        if to:
            to_value = list(map(str.strip, to.split(',')))
        else:
            to_value = [APP_ID]
        body = self.query_one("#task_body", TextArea)
        body_val = body.text.strip()
        body.text = ""
        return BeamTask(
            from_=APP_ID,
            to=to_value,
            metadata=self._parse_json_or_text(self.query_one("#task_metadata", Input).value) or None,
            ttl=self.query_one("#task_ttl", Input).value.strip() or "30s",
            body=body_val,
            failure_strategy="discard"
        )

    @staticmethod
    def _parse_json_or_text(value: str) -> Any:
        try:
            return json.loads(value) if value else {}
        except Exception:
            return value

class IncomingTasksTab(TabPane):
    BINDINGS = [Binding("r", "refresh", "Refresh Results"), Binding("ctrl+enter", "submit_result", "Submit Result")]

    class TaskList(Static):
        current_tasks: dict[UUID4, BeamTask] = {}

        async def on_mount(self) -> None:
            self.table = DataTable()
            self.mount(self.table)
            self.table.add_columns("ID", "From", "Body")
            self.task_rows = {}  # Map task ID to row key
            asyncio.create_task(self.watch_tasks())

        def update_tasks(self, new_tasks: List[BeamTask]):
            for task in new_tasks:
                if task.id not in self.task_rows:
                    row_key = self.table.add_row(task.id, task.from_, task.body)
                    self.task_rows[task.id] = row_key

        async def watch_tasks(self) -> None:
            while True:
                try:
                    new_tasks = await BEAM_CLIENT.get_beam_tasks()
                    self.current_tasks = {task.id: task for task in new_tasks}
                    self.update_tasks(new_tasks)
                except Exception as e:
                    debug(f"Error fetching tasks: {e}")

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Vertical(
                Label("Incoming Tasks"),
                self.TaskList(),
                id="incoming_tasks_upper",
            )
        )
        yield Vertical(
            Label("Create & Send Result"),
            Horizontal(
                Vertical(Label("metadata"), Input(placeholder='{"source":"tui"}', id="res_metadata")),
                Vertical(
                    Label("status"),
                    Select(
                        (
                            ("success", "success"),
                            ("claimed", "claimed"),
                            ("tempfailed", "tempfailed"),
                            ("permfailed", "permfailed"),
                        ),
                        id="res_status",
                    ),
                ),
                id="res_form_row1",
            ),
            Vertical(Label("body"), TextArea(placeholder="result body", id="res_body")),
            Button("Submit Result", id="res_submit", variant="primary"),
            id="results_lower",
        )


    @on(Button.Pressed, "#res_submit")
    def _on_res_submit(self) -> None:
        payload = self._collect_form()
        lv = self.query_one("#incoming_tasks_list", ListView)
        lv.append(ListItem(Label(json.dumps(payload)[:120])))

    def action_submit_result(self) -> None:
        self._on_res_submit()

    def _collect_form(self) -> dict[str, Any]:
        meta_raw = self.query_one("#res_metadata", Input).value
        try:
            metadata = json.loads(meta_raw) if meta_raw else {}
        except Exception:
            metadata = meta_raw
        status = self.query_one("#res_status", Select).value or "success"
        body = self.query_one("#res_body", TextArea).text.strip()
        return {"metadata": metadata, "status": status, "body": body}


class TasksResultsApp(App):
    CSS = """
    #tasks_upper, #incoming_tasks_upper {
        height: 1fr;
        border: tall $surface;
        padding: 1;
    }
    #tasks_upper_left, #tasks_upper_right {
        width: 1fr;
        height: 100%;
        border: panel $accent;
        padding: 1;
    }
    #tasks_lower, #results_lower {
        height: 25;
        border: panel $surface;
        padding: 1;
    }
    TextArea {
        border: round $accent;
    }
    Button {
        width: 20;
    }
    """

    TITLE = "Tasks & Results"
    tasks: reactive[dict[UUID4, BeamTask]] = reactive({})

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with TabbedContent():
            with TasksTab("Outgoing Tasks"):
                pass
            with IncomingTasksTab("Incoming Tasks"):
                pass
        yield Footer()

    def __init__(self):
        super().__init__()

if __name__ == "__main__":
    app = TasksResultsApp()
    app.run()
