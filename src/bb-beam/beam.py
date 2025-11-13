import asyncio
from pathlib import Path
from typing import Any, List, Literal, Optional
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, ConfigDict, Field


class Retry(BaseModel):
    backoff_millisecs: int
    max_tries: int


class FailureStrategy(BaseModel):
    retry: Retry


class BeamTask(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: UUID = Field(default_factory=uuid4)
    from_: str = Field(alias="from")
    to: List[str]
    body: str
    failure_strategy: FailureStrategy | Literal["discard"]
    ttl: str
    metadata: Optional[Any] = None


BeamWorkStatus = Literal["claimed", "succeeded", "tempfailed", "permfailed"]


class BeamResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    task: UUID
    from_: str = Field(alias="from")
    to: List[str]
    body: str
    status: BeamWorkStatus
    metadata: Optional[Any] = None


class FileMetadata(BaseModel):
    suggested_name: str
    id: str = Field(alias="meta")


class BeamSocketRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: UUID
    from_: str = Field(alias="from")
    to: List[str]
    metadata: FileMetadata


class BeamClient:
    def __init__(self, app_id, beam_apikey, beam_proxy_url):
        self.base_url = beam_proxy_url
        self.app_id = app_id
        self.api_key = beam_apikey
        self.client = httpx.AsyncClient(
            headers={"Authorization": f"ApiKey {app_id} {beam_apikey}"}
        )

    async def post_beam_task(self, task: BeamTask):
        json = task.model_dump_json(by_alias=True)
        response = await self.client.post(f"{self.base_url}/v1/tasks", content=json)
        response.raise_for_status()

    async def get_beam_tasks(self) -> list[BeamTask]:
        response = await self.client.get(
            f"{self.base_url}/v1/tasks", params={"filter": "todo", "wait_time": "1s"}
        )
        response.raise_for_status()
        tasks_data = response.json()
        beam_tasks = [BeamTask.model_validate(task_data) for task_data in tasks_data]
        return beam_tasks

    async def answer_task(
        self,
        task: BeamTask,
        body: str,
        workstatus: BeamWorkStatus = "succeeded",
        metadata: Optional[Any] = None,
    ):
        result = BeamResult(
            task=task.id,
            from_=self.app_id,
            to=[task.from_],
            body=body,
            status=workstatus,
            metadata=metadata,
        )
        response = await self.client.put(
            f"{self.base_url}/v1/tasks/{task.id}/results/{self.app_id}",
            content=result.model_dump_json(by_alias=True),
        )
        response.raise_for_status()

    async def get_task_results(self, task_id: UUID) -> list[BeamResult]:
        response = await self.client.get(
            f"{self.base_url}/v1/tasks/{task_id}/results", params={"wait_time": "1s"}
        )
        if response.status_code == 404:
            raise ValueError("Task gone")
        response.raise_for_status()
        result_json = response.json()
        return [BeamResult.model_validate(result) for result in result_json]

    async def upload_file_for(self, task: BeamTask, file_path: Path):
        from main import debug
        to = ".".join(task.from_.split(".")[:2])
        proc = await asyncio.subprocess.create_subprocess_exec(
            "docker",
            "run",
            "--rm",
            "--net=host",
            "-v",
            f"{file_path.resolve()}:{file_path.resolve()}",
            "samply/beam-file",
            "--beam-url",
            self.base_url,
            "--beam-secret",
            self.api_key,
            "--beam-id",
            self.app_id,
            "send",
            "--meta",
            f'"{task.id}"',
            "--to",
            to,
            f"{file_path.resolve()}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        if (await proc.wait()) != 0:
            stdout = await proc.stdout.read()
            debug("Error", stdout.decode())

    async def get_socket_request(self) -> BeamSocketRequest | None:
        response = await self.client.get(
            f"{self.base_url}/v1/sockets",
            params={"wait_count": "1", "wait_time": "10s"},
            timeout=12,
        )
        response.raise_for_status()
        json = response.json()
        if len(json) == 0:
            return None
        return BeamSocketRequest.model_validate(json[0])

    async def download_file_for(self, socket_request: BeamSocketRequest) -> bytes:
        from main import debug
        proc = await asyncio.subprocess.create_subprocess_exec(
            "docker",
            "run",
            "--rm",
            "--net=host",
            "samply/beam-file",
            "--beam-url",
            self.base_url,
            "--beam-secret",
            self.api_key,
            "--beam-id",
            self.app_id,
            "receive",
            "-n",
            "1",
            "print",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        if (await proc.wait()) != 0:
            stderr = await proc.stderr.read()
            debug("Error", stderr.decode())
        return await proc.stdout.read()
