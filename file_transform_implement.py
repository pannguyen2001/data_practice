# %%
import json
import os
import re
import time
import traceback
from argparse import FileType
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime
from enum import Enum, StrEnum, auto
from importlib.abc import PathEntryFinder
from logging import warning
from pathlib import Path
from tokenize import group
from typing import Callable, Dict, List, Optional, Set

import duckdb
from _duckdb import checkpoint
from jinja2 import pass_context
from loguru import logger
from natsort import natsorted
from pydantic import Field, field_validator
from pydantic.dataclasses import dataclass
from python_calamine import CalamineWorkbook

import polars as pl
from file_transformer import ParallelConverter

# %%
# =================== Final pipeline ======================
date_today = datetime.now().strftime("%Y%m%d")
datetime_today = datetime.now().strftime("%Y%m%d_%H%M%S")


class FileTypeEnum(StrEnum):
    CSV = auto()
    EXCEL = auto()
    JSON = auto()
    PARQUET = auto()
    SQL = auto()
    DUCKDB = auto()


# %%
class OutputFormat(Enum):
    """Supported output formats."""

    CSV = "csv"
    DUCKDB = "duckdb"  # Native DuckDB table
    JSON = "json"
    PARQUET = "parquet"
    SQL = "sql"  # DuckDB/SQLite export


@dataclass(frozen=True)
class FileConfig:
    file_name: str
    pattern: str = ""
    ignore_sheets: list[str] = Field(default_factory=list)


@dataclass
class FileGroup:
    config: FileConfig
    file_paths: list[Path] = Field(default_factory=list)
    sheets: list[str] = Field(default_factory=list)


class FileTypeDetection:
    _file_type_dict: dict[str, FileTypeEnum] = {
        ".csv": FileTypeEnum.CSV,
        ".duckdb": FileTypeEnum.DUCKDB,
        ".json": FileTypeEnum.JSON,
        ".jsonl": FileTypeEnum.JSON,
        ".parquet": FileTypeEnum.PARQUET,
        ".pq": FileTypeEnum.PARQUET,
        ".sql": FileTypeEnum.SQL,
        ".xlsx": FileTypeEnum.EXCEL,
        ".xls": FileTypeEnum.EXCEL,
    }

    @classmethod
    def detect(cls, file_path: Path) -> FileTypeEnum | None:
        return cls._file_type_dict.get(file_path.suffix.lower())


class TaskState(StrEnum):
    RUNNING = auto()
    SUCCESS = auto()
    FAILED = auto()


@dataclass
class TaskStatus:
    run_id: str
    task_id: str
    status: TaskState
    started_at: str | None = None
    finished_at: str | None = None
    duration: float | None = None
    error: str | None = None
    trasfer_result: list[dict] = Field(default_factory=list[dict])


@dataclass
class Metrics:
    files_read: int = 0
    files_written: int = 0
    # rows_processed: int = 0
    retries: int = 0

    success: int = 0
    failed: int = 0

    duration: float = 0


@dataclass
class TaskResult:
    status: TaskStatus
    metrics: Metrics


@dataclass(frozen=True)
class WriteOptions:
    chunked: bool = False
    chunk_size: int = 100_000
    deduplicate: bool = False


def retry(
    retries=3,
    delay=2,
):
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    logger.warning(
                        f"{func.__name__} failed attempt {attempt + 1}/{retries}"
                    )
                    time.sleep(delay)
            raise last_error

        return wrapper

    return decorator


# Pipeline
# Get all file in folder
@dataclass
class FileCollector:
    folder_path: Path

    @field_validator("folder_path")
    @classmethod
    def is_valid(cls, folder_path: Path) -> Path:
        if not folder_path.exists():
            raise ValueError(f"{folder_path} does not exist.")

        if not folder_path.is_dir():
            raise ValueError(f"{folder_path} is not folder.")

        return folder_path

    def collect(self) -> list[Path]:
        files: list[Path] = []

        for file in self.folder_path.iterdir():
            if not file.is_file():
                logger.warning(f"'{str(file)}' is not file. Skipped.")
                continue

            if FileTypeDetection.detect(file) is None:
                logger.warning(f"'{str(file)}' is not supported. Skipped.")
                continue

            files.append(file)

        logger.success(
            f"Get files in folder '{self.folder_path}' successfully. "
            f"Num of files: {len(files)}"
        )
        return files


@dataclass
class FileGrouper:
    files: list[Path]
    configs: list[FileConfig]

    def group(self) -> list[FileGroup]:
        result: list[FileGroup] = []

        for config in self.configs:
            file_paths: list[Path] = []

            for file in self.files:
                if config.pattern:
                    matched_group = re.match(config.pattern, file.stem)
                    if matched_group:
                        matched = True
                    else:
                        matched = False
                else:
                    matched = config.file_name in file.stem

                if file.is_file() and matched:
                    file_paths.append(file)

            if not file_paths:
                logger.warning(
                    f"'{config.file_name}' has no matched files to group. No record in result."
                )
                continue

            result.append(FileGroup(config, file_paths=file_paths, sheets=[]))

            logger.success(
                "Group '{}' ({} files):\n- {}.".format(
                    config.file_name,
                    len(file_paths),
                    "\n- ".join([f.name for f in file_paths]),
                )
            )

        return result


@dataclass
class SheetGrouper:
    config: list[FileGroup]

    def group(self) -> list[FileGroup]:
        if not self.config:
            logger.warning("No grouped files.")
            return []

        for item in self.config:
            ignore_sheets: set[str] = set(item.config.ignore_sheets)
            files: list[Path] = item.file_paths
            sheet_list: list[list[str]] = []

            for file in files:
                file_suffix = FileTypeDetection.detect(file)
                if file_suffix != FileTypeEnum.EXCEL:
                    logger.warning(
                        f"'{str(file)}' is not excel file. Skip grouping sheet."
                    )
                    continue
                try:
                    sheets = CalamineWorkbook.from_path(str(file)).sheet_names
                    sheet_list.append(sheets)
                except Exception:
                    logger.exception(f"Failed to load sheet names from file '{file}'.")
                    continue

            if not sheet_list:
                logger.warning(
                    f"'{item.config.file_name}' has no common sheets in grouped files."
                )
                continue
            elif len(sheet_list) == 1:
                grouped_sheets = set(sheet_list[0])
            else:
                grouped_sheets = set(sheet_list[0]).intersection(*sheet_list[1:])
            grouped_sheets = natsorted(grouped_sheets - ignore_sheets)

            item.sheets = grouped_sheets

            logger.success(
                "Group sheet for object {} ({} sheets):\n- {}.".format(
                    item.config.file_name, len(item.sheets), "\n- ".join(item.sheets)
                )
            )

        return self.config


# ── Readers (Strategy Pattern) ──────────────────────────────────────────────
class BaseReader:
    """Abstract base for file readers."""

    def read(self, file_path: Path, **kwargs) -> pl.LazyFrame:
        raise NotImplementedError


class CSVReader(BaseReader):
    def read(self, file_path: Path, **kwargs) -> pl.LazyFrame:
        return pl.scan_csv(file_path, infer_schema_length=0, has_header=True, **kwargs)


class ParquetReader(BaseReader):
    def read(self, file_path: Path, **kwargs) -> pl.LazyFrame:
        return pl.scan_parquet(file_path, **kwargs)


class JSONReader(BaseReader):
    def read(self, file_path: Path, **kwargs) -> pl.LazyFrame:
        first_char = self._detect_first_char(file_path)

        if first_char == "[":
            return pl.read_json(file_path).lazy()

        if first_char == "{":
            return self._read_object_or_ndjson(file_path, **kwargs)

        # Unrecognized — attempt NDJSON but warn explicitly
        logger.warning(
            "JSONReader: unrecognized first char %r in %s, defaulting to NDJSON",
            first_char,
            file_path,
        )
        return pl.scan_ndjson(file_path, **kwargs)

    # ------------------------------------------------------------------

    def _detect_first_char(self, file_path: Path) -> str:
        with open(file_path, "r", encoding="utf-8-sig") as f:  # utf-8-sig strips BOM
            for line in f:
                stripped = line.strip()
                if stripped:
                    return stripped[0]
        raise ValueError(f"JSONReader: file is empty or whitespace-only: {file_path}")

    def _read_object_or_ndjson(self, file_path: Path, **kwargs) -> pl.LazyFrame:
        # Single scan — hold the reference, do not call twice
        try:
            lazy = pl.scan_ndjson(file_path, **kwargs)
            _ = lazy.schema  # probe: raises if not valid NDJSON
            return lazy
        except pl.exceptions.ComputeError:
            # Specifically a Polars parse failure — treat as single JSON object
            logger.debug(
                "JSONReader: NDJSON parse failed for %s, falling back to read_json",
                file_path,
            )
            return pl.read_json(file_path).lazy()


class ExcelReader(BaseReader):
    def read(self, file_path: Path, **kwargs) -> pl.LazyFrame:

        return pl.read_excel(
            file_path,
            engine="calamine",
            infer_schema_length=0,
            has_header=True,
            **kwargs,
        ).lazy()


class ReaderFactory:
    """Factory to get appropriate reader."""

    _readers: Dict[FileTypeEnum, BaseReader] = {
        FileTypeEnum.CSV: CSVReader(),
        FileTypeEnum.PARQUET: ParquetReader(),
        FileTypeEnum.JSON: JSONReader(),
        FileTypeEnum.EXCEL: ExcelReader(),
    }

    @classmethod
    def get_reader(cls, file_type: FileTypeEnum) -> BaseReader:
        if file_type not in cls._readers:
            raise ValueError(f"No reader for type: {file_type}")
        return cls._readers[file_type]


reader_factory = ReaderFactory()


# ── Writers (Strategy Pattern) ──────────────────────────────────────────────
class BaseWriter:
    """Abstract base for output writers."""

    def write(
        self, lf: pl.LazyFrame, output_path: Path, options: WriteOptions, **kwargs
    ) -> Path:
        raise NotImplementedError


class ParquetWriter(BaseWriter):
    def write(
        self, lf: pl.LazyFrame, output_path: Path, options: WriteOptions, **kwargs
    ) -> Path:
        if options.chunked:
            lf.sink_parquet(
                output_path,
                compression="zstd",
                row_group_size=options.chunk_size,
            )
        else:
            lf.sink_parquet(output_path)

        return output_path


class CSVWriter(BaseWriter):
    def write(
        self, lf: pl.LazyFrame, output_path: Path, options: WriteOptions, **kwargs
    ) -> Path:
        if options.chunked:
            lf.sink_csv(
                output_path,
                batch_size=options.chunk_size,
            )
        else:
            lf.sink_csv(output_path)

        return output_path


class SQLWriter(BaseWriter):
    """Write to SQL via DuckDB."""

    def __init__(self, connection_string: Optional[str] = None):
        self.conn_str = connection_string or ":memory:"

    def write(
        self, lf: pl.LazyFrame, output_path: Path, options: WriteOptions, **kwargs
    ) -> Path:
        # Convert to DuckDB table
        df = lf.collect()
        conn = duckdb.connect(
            str(output_path) if str(output_path).endswith(".duckdb") else ":memory:"
        )

        table_name = kwargs.get("table_name", "data")
        safe_name = re.sub(r"\W+", "_", table_name)
        conn.register("tmp_df", df)

        conn.execute(
            f"""
            CREATE OR REPLACE TABLE {safe_name}
            AS SELECT * FROM tmp_df
            """
        )
        conn.close()
        return output_path


class JSONWriter(BaseWriter):
    def write(
        self, lf: pl.LazyFrame, output_path: Path, options: WriteOptions, **kwargs
    ) -> Path:
        lf.sink_ndjson(output_path)
        return output_path


class WriterFactory:
    """Factory to get appropriate writer."""

    _writers: Dict[OutputFormat, BaseWriter] = {
        OutputFormat.PARQUET: ParquetWriter(),
        OutputFormat.CSV: CSVWriter(),
        OutputFormat.JSON: JSONWriter(),
        OutputFormat.SQL: SQLWriter(),
        OutputFormat.DUCKDB: SQLWriter(),
    }

    @classmethod
    def get_writer(cls, fmt: OutputFormat) -> BaseWriter:
        if fmt not in cls._writers:
            raise ValueError(f"No writer for format: {fmt}")
        return cls._writers[fmt]


writer_factory = WriterFactory()


@dataclass
class OutputDataInfo:
    input_file: str
    output_file: Path
    sheet_name: str | None = None
    schema: list[dict] = Field(default_factory=list)
    rows: int = 0
    columns: int = 0
    size: float = 0

    @classmethod
    def to_dict(cls) -> dict:
        return {
            "input_file": cls.input_file,
            "output_file": str(cls.output_file),
            "sheet_name": cls.sheet_name,
            "rows": cls.rows,
            "columns": cls.columns,
            "schema": cls.schema,
            "size_mb": cls.size,
        }


@dataclass
class FileTypeTransfer:
    groups: List[FileGroup]
    output_file_suffix: OutputFormat
    output_path: Path
    write_options: WriteOptions = Field(default_factory=WriteOptions)
    output_data_info: list[dict] = Field(default_factory=list)

    def read_file(self, group: FileGroup) -> pl.LazyFrame | None:
        result = []
        for file in group.file_paths:
            file_suffix = FileTypeDetection.detect(file)
            if file_suffix is None:
                continue
            reader = reader_factory.get_reader(file_suffix)
            df = reader.read(file)
            result.append(df)
            logger.success(f"Read file '{str(file)}' successfully.")
        if not result:
            logger.warning(f"No readable data found for '{group.config.file_name}'.")
            return None

        return pl.concat(result, how="diagonal")

    def read_sheet(self, group: FileGroup) -> dict[str, pl.LazyFrame | None]:
        result: dict[str, pl.LazyFrame | None] = {}

        for sheet in group.sheets:
            temp_res = []

            for file in group.file_paths:
                reader = reader_factory.get_reader(FileTypeEnum.EXCEL)
                try:
                    temp_df = reader.read(file, sheet_name=sheet)
                    temp_res.append(temp_df)
                    logger.success(
                        f"Read sheet '{sheet}' from file '{file}' successfully."
                    )
                except Exception:
                    logger.exception(f"Failed reading '{sheet}' from '{file}'.")
                    continue

            if not temp_res:
                logger.warning(f"No data found for sheet '{sheet}'.")
                continue

            result[sheet] = pl.concat(temp_res, how="diagonal")

        return result

    def write(self, df: pl.LazyFrame, output_path: Path, **kwargs) -> tuple:
        try:
            if df.limit(1).collect().is_empty():
                logger.warning(f"No data to write for '{output_path}'. Skipped.")
                return (0, 0, None, 0)

            if self.write_options.deduplicate:
                df = df.unique()
            writer = writer_factory.get_writer(self.output_file_suffix)
            writer.write(df, output_path, self.write_options, **kwargs)

            rows = df.select(pl.len()).collect().item()
            columns = len(df.collect_schema().names())
            schema = (
                pl.DataFrame(df.collect_schema()).to_pandas().to_dict(orient="records")
            )
            size = round(os.path.getsize(output_path) / (1024**2), 4)

            logger.success(
                f"Write data successfully. "
                f"Output file: '{output_path}'. "
                f"Output file size(MB): {size}. "
                f"Rows: {rows}. "
                f"Columns: {columns}."
            )

            return (rows, columns, schema, size)
        except Exception as e:
            logger.error(traceback.TracebackException.from_exception(e))
            return (0, 0, None, 0)

    def transfer(self, **kwargs):
        output_dir = Path(self.output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        for group in self.groups:
            if group.sheets:
                dfs = self.read_sheet(group)

                for sheet_name, df in dfs.items():
                    output_path = (
                        Path(self.output_path)
                        / f"{group.config.file_name}.{sheet_name}.{self.output_file_suffix.value}"
                    )

                    temp_output_data_info = OutputDataInfo(
                        input_file=", ".join(
                            list(map(lambda x: str(x), group.file_paths))
                        ),
                        output_file=output_path,
                        sheet_name=sheet_name,
                    )

                    if df is None:
                        logger.warning("No data to write. Skipped.")
                        continue
                    result = self.write(
                        df, output_path, sheet_name=sheet_name, **kwargs
                    )

                    (
                        temp_output_data_info.rows,
                        temp_output_data_info.columns,
                        temp_output_data_info.schema,
                        temp_output_data_info.size,
                    ) = result
                    self.output_data_info.append(asdict(temp_output_data_info))
            else:
                output_path = (
                    Path(self.output_path)
                    / f"{group.config.file_name}.{self.output_file_suffix.value}"
                )
                temp_output_data_info = OutputDataInfo(
                    input_file=str(group.file_paths),
                    output_file=output_path,
                )

                df = self.read_file(group)
                if df is None:
                    logger.warning("No data to write. Skipped.")
                    continue

                result = self.write(df, output_path, **kwargs)

                (
                    temp_output_data_info.rows,
                    temp_output_data_info.columns,
                    temp_output_data_info.schema,
                    temp_output_data_info.size,
                ) = result
                self.output_data_info.append(asdict(temp_output_data_info))

        return self.output_data_info


from datetime import datetime


# @retry(retries=3)
def _transfer_group(group: FileGroup, output_format: OutputFormat, output_path: Path):

    metrics = Metrics()
    status = TaskStatus(
        run_id=datetime_today,
        task_id=group.config.file_name,
        status=TaskState.RUNNING,
        started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    start = time.perf_counter()

    try:
        transferer = FileTypeTransfer(
            [group], output_format, output_path, WriteOptions(deduplicate=True)
        )
        result = transferer.transfer()

        metrics.files_read += len(group.file_paths)
        metrics.files_written += 1
        status.trasfer_result = result
        status.status = TaskState.SUCCESS
        metrics.success = 1

    except Exception as e:
        status.status = TaskState.FAILED
        status.error = str(e)
        metrics.failed = 1

    finally:
        status.finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status.duration = metrics.duration = round(time.perf_counter() - start, 4)

    return TaskResult(
        status=status,
        metrics=metrics,
    )


def save_checkpoint(data, file_path: Path):
    tmp = file_path.with_suffix(".tmp")

    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, default=str)

    tmp.replace(file_path)


def load_checkpoint(file_path: Path):
    if not file_path.exists():
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.touch()

    with open(file_path, "r+") as f:
        try:
            return json.load(f)  # Dùng json.load cho file object
        except json.JSONDecodeError:
            return {}  # Trả về dict rỗng nếu file trống hoặc lỗi định dạng


@dataclass
class FileConversionPipeline:
    folder: Path
    configs: List[FileConfig]
    output_path: Path
    output_format: OutputFormat
    checkpoint_file: Path

    def run(self) -> list[FileGroup]:
        files: list[Path] = FileCollector(self.folder).collect()
        if not files:
            logger.error(f"No file in folder '{self.folder}'.")
            return []

        grouped_files: list[FileGroup] = FileGrouper(files, self.configs).group()
        if not grouped_files:
            logger.warning(f"No file can be grouped in folder '{self.folder}'.")
            return []

        logger.debug(grouped_files)

        grouped_sheets: list[FileGroup] = SheetGrouper(grouped_files).group()
        if not grouped_sheets:
            logger.warning(f"No excel file to group sheet in folder '{self.folder}'.")

        logger.debug(grouped_sheets)

        total_metrics = Metrics()

        # task_statuses = []
        checkpoint = load_checkpoint(self.checkpoint_file)
        max_worker = max(1, min((os.cpu_count() or 2) - 1, len(grouped_sheets)))
        with ProcessPoolExecutor(max_workers=max_worker) as executor:
            futures = {}
            for group in grouped_sheets:
                task_id = group.config.file_name
                if (
                    checkpoint.get(group.config.file_name, {}).get("status")
                    == TaskState.SUCCESS
                ):
                    logger.info(
                        f"Skip '{task_id}' due to runned successfully in previous run."
                    )
                else:
                    futures[
                        executor.submit(
                            _transfer_group, group, self.output_format, self.output_path
                        )
                    ] = group

            for future in as_completed(futures):
                group = futures[future]
                try:
                    result: TaskResult = future.result()

                    checkpoint[group.config.file_name] = asdict(result.status)
                    save_checkpoint(checkpoint, self.checkpoint_file)

                    total_metrics.files_read += result.metrics.files_read
                    total_metrics.files_written += result.metrics.files_written
                    total_metrics.success += result.metrics.success
                    total_metrics.failed += result.metrics.failed
                    total_metrics.retries += result.metrics.retries
                    total_metrics.duration += result.metrics.duration
                except Exception:
                    logger.exception(
                        f"Failed processing group '{group.config.file_name}'"
                    )

        logger.info(total_metrics)
        logger.success(f"Save checkpoint successfully at: {str(self.checkpoint_file)}")

        return grouped_sheets


# %%
# ParallelConverter
if __name__ == "__main__":
    import yaml

    with open(r"file_conversation_config.yaml", "r") as f:
        data = yaml.safe_load(f)
        logger.info(f"Read config data: {data}")

    configs: list[FileConfig] = []

    for file_config in data["file_configs"]:
        configs.append(FileConfig(**file_config))
    logger.info(f"Configs: {configs}")

    folder = Path(r"C:\Users\rian.pham\Downloads\C2R2_cus_data_11_June")
    # folder = Path("./")
    output_folder = Path("./file_conversion_pipeline")
    checkpoint_file = Path(f"./checkpoint/checkpoint_{date_today}.json")

    FileConversionPipeline(
        folder, configs, output_folder, OutputFormat.PARQUET, checkpoint_file
    ).run()


# %%
# if __name__ == "__main__":
#     configs = [
#         FileConfig(
#             file_name="OEE Manufacturing Report",
#         ),
#     ]

#     folder = Path(r"C:\_My_job\_Code\_try python\data\staging\20th")
#     output_folder = Path(r"C:\_My_job\_Code\_try python\data\raw\20th")
#     checkpoint_file = Path(f"./checkpoint/checkpoint_{date_today}.json")

#     FileConversionPipeline(
#         folder, configs, output_folder, OutputFormat.PARQUET, checkpoint_file
#     ).run()
