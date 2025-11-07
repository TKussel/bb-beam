"""
Fixed CSS properties for Textual (replacing unsupported `column-gap` and `row-gap`).
Textual App Skeleton with Two Tabs: (a) Tasks and (b) Results
"""
from __future__ import annotations

import asyncio
import json
import os
from typing import Any, List

import uuid
import httpx
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import (
    Button,
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

class UpdateTasks(Message):
    def __init__(self, items: dict):
        self.items = items
        super().__init__()

RESULTS_ENDPOINT = os.getenv("RESULTS_ENDPOINT", "https://httpbin.org/json")
TASKS_LIST_PLACEHOLDER = [
    "Sent: Task 1",
    "New Task",
]

class SectionTitle(Static):
    def __init__(self, text: str) -> None:
        super().__init__(text)

class NonFocusableVertical(Vertical):
    can_focus=False

class TaskLabel(Label):
    def __init__(self, task_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_id = task_id


class TasksTab(TabPane):
    BINDINGS = [Binding("ctrl+enter", "submit_task", "Submit Task")]

    class Submitted(Message):
        def __init__(self, payload: dict[str, Any]) -> None:
            self.payload = payload
            super().__init__()

    @on(UpdateTasks)
    def update_task_list(self, message: UpdateTasks):
        list_view = self.query_one("#tasks_list", ListView)
        list_view.clear()
        self.notify("Updating task")
        for k, v in message.items.items():
            list_view.append(ListItem(TaskLabel(v.get("id") ,f"{k}")))

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Vertical(
                SectionTitle("Sent Tasks"),
                ListView(*[ListItem(TaskLabel(v.get("id"), k)) for (k, v) in self.app.tasks.items()], id="tasks_list"),
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
        self.notify(f"{label}")
        self.query_one("#tasks_preview", Pretty).update(self.app.tasks.get(label.content))

    @on(Button.Pressed, "#task_submit")
    def _on_task_submit(self) -> None:
        payload = self._collect_form()
        self.post_message(self.Submitted(payload))
        tasks_list = self.query_one("#tasks_list", ListView)
        tasks_list.append(ListItem(Label(f"Sent: {payload.get('id','?')}")))

    def action_submit_task(self) -> None:
        self._on_task_submit()

    def _collect_form(self) -> dict[str, Any]:
        return {
            "id": uuid.uuid4(),
            "to": [x.strip() for x in self.query_one("#task_to", Input).value.split(',')],
            "metadata": self._parse_json_or_text(self.query_one("#task_metadata", Input).value),
            "ttl": self.query_one("#task_ttl", Input).value.strip(),
            "body": self.query_one("#task_body", TextArea).text.strip(),
        }

    @staticmethod
    def _parse_json_or_text(value: str) -> Any:
        try:
            return json.loads(value) if value else {}
        except Exception:
            return value

class ResultsTab(TabPane):
    BINDINGS = [Binding("r", "refresh", "Refresh Results"), Binding("ctrl+enter", "submit_result", "Submit Result")]

    results: reactive[List[dict[str, Any]]] = reactive([], layout=True)

    class Submitted(Message):
        def __init__(self, payload: dict[str, Any]) -> None:
            self.payload = payload
            super().__init__()

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Vertical(
                Label("Incoming Results"),
                ListView(id="results_list"),
                id="results_upper",
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

    async def on_mount(self) -> None:
        await self._load_results()

    async def watch_results(self, results: List[dict[str, Any]]) -> None:
        lv = self.query_one("#results_list", ListView)
        lv.clear()
        for item in results:
            summary = json.dumps(item)[:120]
            lv.append(ListItem(Label(summary)))

    async def _load_results(self) -> None:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(RESULTS_ENDPOINT)
                resp.raise_for_status()
                data = resp.json()
        except Exception as e:
            data = [{"id": 1, "status": "success", "message": f"Demo (error: {e})"}]
        if isinstance(data, list):
            self.results = data
        elif isinstance(data, dict):
            self.results = data.get("items", [data])

    @on(Button.Pressed, "#res_submit")
    def _on_res_submit(self) -> None:
        payload = self._collect_form()
        self.post_message(self.Submitted(payload))
        lv = self.query_one("#results_list", ListView)
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
    #tasks_upper, #results_upper {
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
    tasks = reactive({})

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with TabbedContent():
            with TasksTab("Outgoing Tasks"):
                pass
            with ResultsTab("Incoming Tasks"):
                pass
        yield Footer()

    def __init__(self):
        super().__init__()
        self.tasks = {"Empty": json.loads('{"id": "0000-0000", "text": "Nothing to display"}')}
        self.notify(f"")

    @on(TasksTab.Submitted)
    def _on_task_submitted(self, message: TasksTab.Submitted) -> None:
        self.bell()
        self.tasks[message.payload.get('id')] = message.payload
        self.post_message(UpdateTasks(self.tasks))
        self.notify(f"Task created: {message.payload.get('to','(no to)')}")

    @on(ResultsTab.Submitted)
    def _on_result_submitted(self, message: ResultsTab.Submitted) -> None:
        self.bell()
        self.notify(f"Result submitted: {message.payload.get('status','success')}")

if __name__ == "__main__":
    app = TasksResultsApp()
    app.run()

