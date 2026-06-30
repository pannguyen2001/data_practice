import os
from dotenv import load_dotenv

load_dotenv()
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")

import datetime
import time
from abc import ABC, abstractmethod
from enum import StrEnum, auto
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Literal, ParamSpec, TypeVar

import gdown
from huggingface_hub import hf_hub_download
from kaggle.api.kaggle_api_extended import KaggleApi
from loguru import logger
from pydantic import Field
from pydantic.dataclasses import dataclass

import polars as pl


P = ParamSpec("P")
R = TypeVar("R")


def logger_wrapper(func: Callable[P, R]) -> Callable[P, R]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        with logger.catch(reraise=True):
            return func(*args, **kwargs)

    return wrapper


# ── Config per source ──────────────────────────────────────────────


class DownloadStatusEnum(StrEnum):
    SUCCESS = auto()
    FAILED = auto()
    SKIPPED = auto()
    DRY_RUN = auto()


@dataclass(frozen=True, slots=True)
class DownloadConfig:
    url: str = ""
    path: Path = Field(default_factory=Path)
    id: str = ""


@dataclass(frozen=True)
class DownloadResult:
    source: str
    destination: Path
    status: DownloadStatusEnum
    files_downloaded: list[Path]
    started_at: str
    completed_at: str
    duration_seconds: float


@dataclass(slots=True)
class BaseDownloader(ABC):
    config: DownloadConfig = Field(default_factory=DownloadConfig)
    options: dict[str, Any] = Field(default_factory=dict)
    dry_run: bool = False
    skip_existing: bool = True

    @abstractmethod
    def _download(self) -> None:
        """Download data from url."""
        pass

    @abstractmethod
    def _validate(self) -> None:
        """Validate arguments."""
        pass

    # @logger_wrapper
    def execute(self) -> DownloadResult:
        """Execute dowload progress."""
        try:
            start_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            start_time = time.perf_counter()

            logger.info(
                f"[{self.__class__.__name__}] Downloading data:\n"
                f"- url: '{self.config.url}'\n"
                f"- id: '{self.config.id}'\n"
                f"- destination: '{self.config.path}'"
            )
            self._validate()
            # if self.skip_existing and self.config.path.exists():
            #     logger.info(
            #         f"[{self.__class__.__name__}] Destination exists, skipping."
            #     )
            #     return DownloadResult(
            #         source=self.config.url or self.config.id,
            #         destination=self.config.path,
            #         status=DownloadStatusEnum.SKIPPED,
            #         files_downloaded=[],
            #         started_at=start_at,
            #         completed_at=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            #         duration_seconds=time.perf_counter() - start_time,
            #     )
            # if self.dry_run:
            #     logger.info(
            #         f"[{self.__class__.__name__}] DRY RUN: would download to {self.config.path}"
            #     )
            #     return DownloadResult(
            #         source=self.config.url or self.config.id,
            #         destination=self.config.path,
            #         status=DownloadStatusEnum.DRY_RUN,
            #         files_downloaded=[],
            #         started_at=start_at,
            #         completed_at=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            #         duration_seconds=time.perf_counter() - start_time,
            #     )
            self._download()

            if self.config.path.is_file():
                files = [self.config.path]

            elif self.config.path.is_dir():
                files = list(p for p in self.config.path.rglob("*") if p.is_file())

            else:
                files = []

            logger.success(f"[{self.__class__.__name__}] Complete downloading data.")

            return DownloadResult(
                source=self.config.url or self.config.id,
                destination=self.config.path,
                status=DownloadStatusEnum.SUCCESS,
                files_downloaded=files,
                started_at=start_at,
                completed_at=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                duration_seconds=time.perf_counter() - start_time,
            )
        except Exception:
            logger.exception(f"[{self.__class__.__name__}] Download failed.")
            raise


@dataclass(slots=True)
class GoogleDriveDownloader(BaseDownloader):
    item_type: Literal["file", "folder"] | None = None
    quiet: bool = False

    def _validate(self) -> None:
        if not self.config.path:
            raise ValueError("Need provide path for download.")
        if self.item_type not in ("file", "folder"):
            raise ValueError("type must be 'file' or 'folder'")
        if self.item_type == "folder":
            if not self.config.url and not self.config.id:
                raise ValueError("Need provide url or id for folder download.")
        else:
            if not self.config.url:
                raise ValueError("Need provide url for file download.")

    def _download_folder(self) -> None:
        if self.config.url:
            gdown.download_folder(
                url=self.config.url,
                output=str(self.config.path),
                **self.options,
            )
            return
        if self.config.id:
            gdown.download_folder(
                id=self.config.id,
                output=str(self.config.path),
                **self.options,
            )
            return

    def _download_file(self) -> None:
        gdown.download(
            url=self.config.url, output=str(self.config.path), **self.options
        )

    def _download(self) -> None:
        if self.item_type == "folder":
            self._download_folder()
        else:
            self._download_file()


class HuggingFaceDownloader(BaseDownloader):
    def _validate(self) -> None:
        if not self.config.id:
            raise ValueError("id is required")
        if not self.config.path:
            raise ValueError("path is required")
        if not self.options.get("file_name"):
            raise ValueError(
                "file_name is required in kwargs for HuggingFaceDownloader"
            )

    def _save_to_parquet(self, file_path: str) -> None:
        pl.scan_parquet(file_path).sink_parquet(str(self.config.path), mkdir=True)

    def _download(self) -> None:
        file_path = hf_hub_download(
            repo_id=self.config.id,
            filename=self.options.get("file_name"),
            repo_type="dataset",
        )
        logger.info(f"Downloaded file from Hugging Face: {file_path}")

        self._save_to_parquet(file_path)


class KaggleDownloader(BaseDownloader):
    _api: KaggleApi | None = None

    def _validate(self) -> None:
        if not self.config.id:
            raise ValueError("Need provide id for kaggle dataset.")
        if not self.config.path:
            raise ValueError("Need provide path for download.")

    @classmethod
    def _get_api(cls) -> KaggleApi:
        if cls._api is None:
            from dotenv import load_dotenv

            load_dotenv()
            api = KaggleApi()
            api.authenticate()
            cls._api = api
        return cls._api

    def _download(self) -> None:
        self._get_api().dataset_download_files(
            self.config.id, path=self.config.path, **self.options
        )


if __name__ == "__main__":
    # Test Google Drive Downloader
    # google_drive_config = DownloadConfig(
    #     id="14vCUXklVCUN_rCPKYN7ys2tDJG72SNYw",
    #     path=r"C:\Users\ASUS\Code\data_practice\data\staging\drive",
    # )
    # google_drive_downloader = GoogleDriveDownloader(
    #     config=google_drive_config,
    #     item_type="folder",
    #     quiet=False,
    # )
    # google_drive_downloader.execute()

    # Test Hugging Face Downloader
    # hugging_face_config = DownloadConfig(
    #     id="tmquan/sapnhap-bando-vn",
    #     path="./data/staging/huggingface/sapnhap-bando-vn.parquet",
    # )
    # hugging_face_downloader = HuggingFaceDownloader(
    #     config=hugging_face_config, options={"file_name": "data/all.parquet"}
    # )
    # hugging_face_downloader.execute()

    # Test Kaggle Downloader
    kaggle_config = DownloadConfig(
        id="muratkokludataset/acoustic-extinguisher-fire-dataset",
        path="./data/staging/kaggle",
    )
    kaggle_downloader = KaggleDownloader(config=kaggle_config, options={"unzip": True})
    kaggle_downloader.execute()
