from __future__ import annotations

from datetime import datetime
from enum import StrEnum, auto
from pathlib import Path
from uuid import UUID, uuid4

from pydantic import Field
from pydantic.dataclasses import dataclass


@dataclass
class SourceConfig:

    name: str
    url: str

    timeout: int = 5
    # source_id: UUID = Field(default_factory=uuid4)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "url": self.url,
            "timeout": self.timeout,
        }



@dataclass
class SourceStatus:
    config: SourceConfig

    reachable: bool = False

    http_status: int | None = None

    content_type: str | None = None

    content_length: int | None = None

    checked_at: datetime = Field(default_factory=datetime.now)

    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "config": self.config.to_dict(),
            "reachable": self.reachable,
            "http_status": self.http_status,
            "content_type": self.content_type,
            "content_length": self.content_length,
            "checked_at": self.checked_at,
            "error": self.error
        }


class DownloadStatus(StrEnum):
    PENDING = auto()
    READY = auto()
    DOWNLOADING = auto()
    SUCCESS = auto()
    FAILED = auto()


@dataclass
class DownloadResult:
    config: SourceConfig
    destination: Path

    download_id: UUID = Field(default_factory=uuid4)

    status: DownloadStatus = DownloadStatus.PENDING

    started_at: datetime = Field(default_factory=datetime.now)

    ended_at: datetime | None = None

    bytes_downloaded: int = 0

    checksum: str | None = None

    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "config": self.config.to_dict(),
            "destination": str(self.destination),
            "download_id": str(self.download_id),
            "status": self.status.value,
            "started_at": self.started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": self.ended_at.strftime("%Y-%m-%d %H:%M:%S") if self.ended_at else None,
            "bytes_downloaded": self.bytes_downloaded,
            "checksum": self.checksum,
            "error": self.error
        }

# %%
from pathlib import Path

import yaml


def read_yaml(file_path: Path | str) -> list[dict]:
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    with file_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)
# source_statuses = read_yaml("source_statuses.yaml")

# %%
import traceback

import requests


def fetch(config: SourceConfig) -> SourceStatus:
    try:
        response = requests.head(
            config.url,
            timeout=config.timeout,
            allow_redirects=True,
        )

        response.raise_for_status()

        length = response.headers.get("Content-Length")

        return SourceStatus(
            config=config,
            reachable=True,
            http_status=response.status_code,
            content_type=response.headers.get("Content-Type"),
            content_length=int(length) if length else None,
        )

    except Exception as e:
        response = getattr(e, "response", None)

        return SourceStatus(
            config=config,
            reachable=False,
            http_status=getattr(response, "status_code", None),
            error="".join(
                traceback.TracebackException.from_exception(e).format()
            ),
        )


# %%
import os
from concurrent.futures import ThreadPoolExecutor


def check_source_status(
    source_configs: list[SourceConfig],
) -> list[SourceStatus]:

    if not source_configs:
        return []

    max_workers = max(
        1,
        min(
            len(source_configs),
            (os.cpu_count() or 2) - 1,
        ),
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        futures = [
            executor.submit(fetch, cfg)
            for cfg in source_configs
        ]

        return [
            future.result()
            for future in futures
        ]


# %%
from pathlib import Path


RAW_DIR = Path("./data/raw")

configs = [
    SourceConfig(**cfg)
    for cfg in read_yaml(Path("source_config.yaml"))
]

statuses = check_source_status(configs)

download_jobs = [
    DownloadResult(
        config=status.config,
        destination=RAW_DIR,
        status=DownloadStatus.READY,
    )
    for status in statuses
    if status.reachable
]

import json

print(json.dumps(list(map(lambda x: x.to_dict(), download_jobs)), indent=4))