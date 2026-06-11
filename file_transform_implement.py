# %%
import os
import re
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from enum import Enum, StrEnum, auto
from pathlib import Path
from tokenize import group
from typing import Callable, Dict, List, Optional, Set

import duckdb
import pandas as pd
import polars as pl
from loguru import logger

# %%
input_dir = Path(r"C:\Users\rian.pham\Downloads\user_info_C2R2_1_may")
files = [f for f in input_dir.iterdir() if f.is_file()]


# %%
# detect file type
class FileTypeEnum(StrEnum):
    CSV = auto()
    EXCEL = auto()
    JSON = auto()
    PARQUET = auto()
    DB = auto()


class FileTypeDetection:
    _file_type_dict: dict[str, FileTypeEnum] = {
        ".csv": FileTypeEnum.CSV,
        ".db": FileTypeEnum.DB,
        ".json": FileTypeEnum.JSON,
        ".jsonl": FileTypeEnum.JSON,
        ".parquet": FileTypeEnum.PARQUET,
        ".pq": FileTypeEnum.PARQUET,
        ".xlsx": FileTypeEnum.EXCEL,
        ".xls": FileTypeEnum.EXCEL,
    }

    def detect(self, file_path: Path) -> FileTypeEnum | None:
        file_suffix: str = file_path.suffix
        if file_suffix not in self._file_type_dict.keys():
            logger.error(
                f"{file_suffix} is not supported. "
                f"Supported file types: {', '.join(self._file_type_dict.keys())}"
            )
            return
        return FileTypeEnum(self._file_type_dict.get(file_suffix))


FileTypeDetection().detect(Path("test.json"))


# %%
# grouped files has same bussiness object
#
from copy import deepcopy


def group_file(files: list[Path], config: list[dict]) -> list:
    group_dict = deepcopy(config)
    for item in group_dict:
        group_name = item["file_name"]
        item["file_paths"] = [
            file for file in files if file.is_file() and group_name in file.stem
        ]
        if not item["file_paths"]:
            logger.warning(f"'{group_name}' has no file. No record in result")
            group_dict.remove(item)

    return group_dict


config: list[dict] = [
    {
        "file_name": "User Information",
        "ignore_sheets": ["Config Data"],
        "file_paths": [],
        "sheets": [],
    },
    {
        "file_name": "Master Data",
        "ignore_sheets": ["Config Data"],
        "file_paths": [],
        "sheets": [],
    },
]
grouped_files = group_file(files, config)
grouped_files

# %%
# Parse sheet from realtional files, detect which keep and which remove, which diff
from python_calamine import CalamineWorkbook


def group_sheet(grouped_files: list[dict]):
    temp_grouped_files = deepcopy(grouped_files)
    for item in temp_grouped_files:
        ignore_sheets: set[str] = set(item["ignore_sheets"])
        files: list[Path] = item["file_paths"]

        sheet_lists = [
            CalamineWorkbook.from_path(str(file)).sheet_names for file in files
        ]
        grouped_sheets = (
            set(sheet_lists[0]).intersection(*sheet_lists[1:])
            if len(sheet_lists) >= 2
            else set(sheet_lists)
        )

        grouped_sheets = sorted(grouped_sheets - ignore_sheets, key=lambda x: x[0])
        item["sheets"] = grouped_sheets

    return temp_grouped_files


grouped_files = group_sheet(grouped_files)
grouped_files

# %%


def to_parquet(grouped_files: list[dict]):
    for file in grouped_files:
        for sheet in file["sheets"]:
            logger.info(f"Processing {sheet}")
            dfs = [
                pl.read_excel(str(file_path), has_header=True)
                for file_path in file["file_paths"]
            ]
            df = pl.concat(dfs, how="diagonal")
            pl.LazyFrame(df).sink_parquet(f"{sheet}.parquet")
            logger.success(f"Complete {sheet}")


to_parquet(grouped_files)
