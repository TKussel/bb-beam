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

BeamWorkStatus = Literal["claimed", "succeeded", "tempfailed", "permfailed"]

class BeamResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    task: UUID
    from_: str = Field(alias='from')
    to: List[str]
    body: str
    status: BeamWorkStatus
    metadata: Optional[Any] = None


class BeamClient:
    def __init__(self, app_id, beam_apikey, beam_proxy_url):
        self.base_url = beam_proxy_url
        self.app_id = app_id
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

    async def answer_task(self, task: BeamTask, body: str, workstatus: BeamWorkStatus = "succeeded", metadata: Optional[Any] = None):
        result = BeamResult(
            task=task.id,
            from_=self.app_id,
            to=[task.from_],
            body=body,
            status=workstatus,
            metadata=metadata
        )
        response = await self.client.put(f"{self.base_url}/v1/tasks/{task.id}/results/{self.app_id}", content=result.model_dump_json(by_alias=True))
        response.raise_for_status()

    async def get_task_results(self, task_id: UUID) -> list[BeamResult]:
        response = await self.client.get(f"{self.base_url}/v1/tasks/{task_id}/results", params={
            "wait_time": "1s"
        })
        response.raise_for_status()
        result_json = response.json()
        return [BeamResult.model_validate(result) for result in result_json]
