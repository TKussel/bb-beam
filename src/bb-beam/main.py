"""
Fixed CSS properties for Textual (replacing unsupported `column-gap` and `row-gap`).
Textual App Skeleton with Two Tabs: (a) Tasks and (b) Results
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, List

from dotenv import load_dotenv
from pydantic.types import UUID4
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets.data_table import RowKey
from beam import BeamClient, BeamResult, BeamTask, BeamWorkStatus
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

def parse_json_or_text(value: str) -> Any:
    try:
        return json.loads(value) if value else {}
    except Exception:
        return value

def list_files(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    if not os.path.isdir(path):
        return []
    try:
        return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    except PermissionError:
        return []

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

    class TaskPreview(Static):

        def __init__(self, task: BeamTask, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.beam_task = task
            self.task_rows: dict[str, RowKey] = {}
            self.table = DataTable(cursor_type="row")
            self.table.add_columns("From", "Body", "Status", "Files")
            self.result_fetch_task = asyncio.create_task(self.watch_results())

        async def on_mount(self) -> None:
            await self.mount(Pretty(self.beam_task))
            await self.mount(self.table)

        async def on_unmount(self) -> None:
            self.result_fetch_task.cancel()

        def update_results(self, new_results: List[BeamResult]):
            for result in new_results:
                if result.from_ not in self.task_rows:
                    row_key = self.table.add_row(result.from_, result.body, result.status, list_files(f"./files/{result.task}/{result.from_}"))
                    self.task_rows[result.from_] = row_key
                else:
                    row_key = self.task_rows[result.from_]
                    self.table.update_cell(row_key, "Body", result.body)
                    self.table.update_cell(row_key, "Status", result.status)
                    self.table.update_cell(row_key, "Files", list_files(f"./files/{result.task}/{result.from_}"))

        async def watch_results(self) -> None:
            while True:
                try:
                    new_results = await BEAM_CLIENT.get_task_results(self.beam_task.id)
                except ValueError:
                    self.query_one(Pretty).update("Task gone")
                    break
                except Exception as e:
                    debug(f"Error fetching tasks: {e}")
                else:
                    self.update_results(new_results)

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Vertical(
                SectionTitle("Sent Tasks"),
                ListView(*[ListItem(TaskLabel(v.id, k)) for (k, v) in self.app.tasks.items()], id="tasks_list"),
                id="tasks_upper_left",
            ),
            NonFocusableVertical(
                SectionTitle("Preview / Details"),
                Container(Pretty("Select a task from the list to show it and its results here."), id="tasks_preview"),
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
    async def _on_task_list_highlight(self, event: ListView.Highlighted) -> None:
        label = event.item.query_one(TaskLabel)
        task: BeamTask = self.app.tasks.get(label.task_id)
        preview = self.query_one("#tasks_preview", Container)
        await preview.remove_children()
        await preview.mount(self.TaskPreview(task))

    @on(Button.Pressed, "#task_submit")
    async def _on_task_submit(self) -> None:
        task = self._collect_form()
        try:
            await BEAM_CLIENT.post_beam_task(task)
        except Exception as e:
            preview = self.query_one("#tasks_preview", Container)
            await preview.remove_children()
            await preview.mount(Pretty(f"Error: {e}"))
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
            metadata=parse_json_or_text(self.query_one("#task_metadata", Input).value) or None,
            ttl=self.query_one("#task_ttl", Input).value.strip() or "30s",
            body=body_val,
            failure_strategy="discard"
        )


class IncomingTasksTab(TabPane):
    BINDINGS = [Binding("r", "refresh", "Refresh Results"), Binding("ctrl+enter", "submit_result", "Submit Result")]

    class TaskList(Static):
        all_tasks: dict[UUID4, BeamTask] = {}

        async def on_mount(self) -> None:
            self.table = DataTable(cursor_type="row")
            await self.mount(self.table)
            [_id, _from, _body, result_col] = self.table.add_columns("ID", "From", "Body", "Result")
            self.result_col = result_col
            self.task_rows: dict[UUID4, RowKey] = {}
            asyncio.create_task(self.watch_tasks())

        def get_selected_task(self) -> BeamTask | None:
            row_key = self.table.coordinate_to_cell_key(self.table.cursor_coordinate).row_key
            for task_id, stored_row_key in self.task_rows.items():
                if stored_row_key == row_key:
                    return self.all_tasks[task_id]

        def update_task_result(self, task_id: UUID4, result: str):
            row_key = self.task_rows[task_id]
            self.table.update_cell(row_key, self.result_col, result)

        def update_tasks(self, new_tasks: List[BeamTask]):
            for task in new_tasks:
                if task.id not in self.task_rows:
                    row_key = self.table.add_row(task.id, task.from_, task.body, "Awaiting response")
                    self.task_rows[task.id] = row_key

        async def watch_tasks(self) -> None:
            while True:
                try:
                    new_tasks = await BEAM_CLIENT.get_beam_tasks()
                except Exception as e:
                    debug(f"Error fetching tasks: {e}")
                else:
                    self.all_tasks = {**self.all_tasks, **{task.id: task for task in new_tasks}}
                    self.update_tasks(new_tasks)

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
                            ("success", "succeeded"),
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
    async def answer_task(self) -> None:
        task_list = self.query_one(self.TaskList)
        task = task_list.get_selected_task()
        if task is None:
            self.notify("Please select a task")
            return
        body = self.query_one("#res_body", TextArea).text.strip()
        status = self.query_one("#res_status", Select).value
        if status == Select.BLANK:
            status = "succeeded"
        metadata = parse_json_or_text(self.query_one("#res_metadata", Input).value)
        try:
            await BEAM_CLIENT.answer_task(task, body, status, metadata)  # pyright: ignore[reportArgumentType]
        except Exception as e:
            self.notify(f"Failed to submit result: {e}")
        task_list.update_task_result(task.id, str(status))

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

    async def watch_socket_requests(self):
        while True:
            try:
                socket_request = await BEAM_CLIENT.get_socket_request()
            except Exception as e:
                debug(f"Error while watching socket requests: {e}")
                self.notify(f"Error while watching socket requests: {e}")
                await asyncio.sleep(1)
                continue
            if socket_request.metadata.task in self.tasks:
                file = await BEAM_CLIENT.download_file_for(socket_request)
                filepath = Path(f"./files/{socket_request.metadata.task}/{socket_request.from_}")
                filepath.mkdir(exist_ok=True)
                filename = socket_request.metadata.filename
                if ".." in filename or filename == "" or filename == "/":
                    continue
                filepath.joinpath(filename).write_bytes(file)
            else:
                self.notify(f"Unknown socket task {socket_request.metadata.task} from {socket_request.from_}")
                try:
                    # Connect to remove the task socket
                    await BEAM_CLIENT.download_file_for(socket_request)
                except Exception:
                    pass

    def on_mount(self):
        asyncio.create_task(self.watch_socket_requests())

    def __init__(self):
        super().__init__()

if __name__ == "__main__":
    app = TasksResultsApp()
    app.run()
