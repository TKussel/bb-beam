from typing import Any, AsyncGenerator, List, Literal, Optional
from uuid import UUID, uuid4

import httpx
import httpx_sse
from pydantic import BaseModel, ConfigDict, Field


class Retry(BaseModel):
    backoff_millisecs: int
    max_tries: int


class FailureStrategy(BaseModel):
    retry: Retry


class BeamTask(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: UUID = Field(default_factory=uuid4)
    from_: str = Field(alias='from')
    to: List[str]
    body: str
    failure_strategy: FailureStrategy | Literal["discard"]
    ttl: str
    metadata: Optional[Any] = None

class BeamResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    task: UUID
    from_: str = Field(alias='from')
    to: List[str]
    body: str
    workstatus: Literal["claimed"] | Literal["succeeded"] | Literal["temp_failed"] | Literal["perm_failed"]
    metadata: Optional[Any] = None


class BeamClient:
    def __init__(self, app_id, beam_apikey, beam_proxy_url):
        self.base_url = beam_proxy_url
        self.client = httpx.AsyncClient(headers={"Authorization": f"ApiKey {app_id} {beam_apikey}"})

    async def post_beam_task(self, task: BeamTask):
        json = task.model_dump_json(by_alias=True)
        response = await self.client.post(f"{self.base_url}/v1/tasks", content=json)
        response.raise_for_status()

    async def get_beam_tasks(self) -> list[BeamTask]:
        response = await self.client.get(f"{self.base_url}/v1/tasks", params={
            "filter": "todo",
            "wait_time": "1s"
        })
        response.raise_for_status()
        tasks_data = response.json()
        beam_tasks = [BeamTask.model_validate(task_data) for task_data in tasks_data]
        return beam_tasks


    async def stream_task_results(self, task_id: UUID) -> AsyncGenerator[BeamTask, None]:
        async with httpx_sse.aconnect_sse(self.client, "GET", f"{self.base_url}/v1/tasks/{task_id}") as event_stream:
            event_stream.response.raise_for_status()
            async for event in event_stream.aiter_sse():
                yield BeamTask.model_validate_json(event.data)
