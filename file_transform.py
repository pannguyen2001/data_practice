# Kimi code, error now
# 2026-June-10

import os
import re
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set

import duckdb
import pandas as pd
from loguru import logger

import polars as pl

# ── Configuration ────────────────────────────────────────────────────────────
OUTPUT_DIR = Path("converted_output")
OUTPUT_DIR.mkdir(exist_ok=True)


# ── Enums & Data Classes ────────────────────────────────────────────────────
class FileType(Enum):
    """Supported input file types."""

    CSV = auto()
    EXCEL = auto()  # .xlsx, .xls
    PARQUET = auto()
    JSON = auto()
    UNKNOWN = auto()


class OutputFormat(Enum):
    """Supported output formats."""

    PARQUET = "parquet"
    CSV = "csv"
    SQL = "sql"  # DuckDB/SQLite export
    DUCKDB = "duckdb"  # Native DuckDB table
    JSON = "json"


@dataclass
class FileGroup:
    """Represents a group of files sharing the same base name."""

    base_name: str
    files: List[Path]
    detected_type: FileType


@dataclass
class ConversionResult:
    """Result of a single file conversion."""

    source: Path
    output: Optional[Path]
    success: bool
    duration: float
    records: int
    error: Optional[str] = None


# ── File Type Detection ─────────────────────────────────────────────────────
class FileTypeDetector:
    """Auto-detect file type from extension and optionally content sniffing."""

    EXTENSION_MAP: Dict[str, FileType] = {
        ".csv": FileType.CSV,
        ".xlsx": FileType.EXCEL,
        ".xls": FileType.EXCEL,
        ".parquet": FileType.PARQUET,
        ".pq": FileType.PARQUET,
        ".json": FileType.JSON,
        ".jsonl": FileType.JSON,
    }

    @classmethod
    def detect(cls, file_path: Path) -> FileType:
        """Detect file type from extension."""
        ext = file_path.suffix.lower()
        return cls.EXTENSION_MAP.get(ext, FileType.UNKNOWN)

    @classmethod
    def is_supported(cls, file_path: Path) -> bool:
        """Check if file type is supported for conversion."""
        return cls.detect(file_path) != FileType.UNKNOWN


# ── File Grouper ─────────────────────────────────────────────────────────────
class FileGrouper:
    """
    Group files by base name (ignoring suffixes like _1, _2, _v2, etc.).
    Example: 'data_1.csv', 'data_2.csv' → group 'data'
    """

    # Pattern to strip common suffixes: _1, _v2, _2024, (1), etc.
    SUFFIX_PATTERNS = [
        r"[_\s]+v?\d+[\s_]*$",  # _1, _2, _v2, _2024
        r"[\s_]*\(\d+\)[\s_]*$",  # (1), (2)
        r"[_\s]+\d+of\d+[\s_]*$",  # 1of3, 2of3
        r"[_\s]+part\d+[\s_]*$",  # part1, part2
        r"[_\s]+copy[\s_]*$",  # copy
        r"[_\s]+backup[\s_]*$",  # backup
    ]

    def __init__(self, patterns: Optional[List[str]] = None):
        self.patterns = patterns or self.SUFFIX_PATTERNS
        self._compiled = [re.compile(p, re.IGNORECASE) for p in self.patterns]

    def _extract_base_name(self, file_path: Path) -> str:
        """Extract base name by stripping suffixes and extension."""
        stem = file_path.stem
        for pattern in self._compiled:
            stem = pattern.sub("", stem)
        return stem.strip("_").strip()

    def group_files(self, files: List[Path]) -> Dict[str, FileGroup]:
        """
        Group files by base name.
        Returns dict: {base_name: FileGroup}
        """
        groups: Dict[str, List[Path]] = {}

        for file_path in files:
            if not FileTypeDetector.is_supported(file_path):
                logger.warning(f"Skipping unsupported file: {file_path}")
                continue

            base = self._extract_base_name(file_path)
            groups.setdefault(base, []).append(file_path)

        # Create FileGroup objects with detected type (majority vote or first file)
        result = {}
        for base_name, file_list in groups.items():
            # Detect type from first file (assumes same type within group)
            detected = FileTypeDetector.detect(file_list[0])
            result[base_name] = FileGroup(
                base_name=base_name, files=file_list, detected_type=detected
            )
            logger.info(
                f"Group '{base_name}': {len(file_list)} files, "
                f"type={detected.name}, files={[f.name for f in file_list]}"
            )

        return result


# ── Readers (Strategy Pattern) ──────────────────────────────────────────────
class BaseReader:
    """Abstract base for file readers."""

    def read(self, file_path: Path) -> pl.LazyFrame:
        raise NotImplementedError

    def count_rows(self, file_path: Path) -> int:
        """Fast row count without full load."""
        raise NotImplementedError


class CSVReader(BaseReader):
    def read(self, file_path: Path) -> pl.LazyFrame:
        return pl.scan_csv(file_path)

    def count_rows(self, file_path: Path) -> int:
        # Fast line count
        with open(file_path, "rb") as f:
            return sum(1 for _ in f) - 1  # minus header


class ParquetReader(BaseReader):
    def read(self, file_path: Path) -> pl.LazyFrame:
        return pl.scan_parquet(file_path)

    def count_rows(self, file_path: Path) -> int:
        return pl.scan_parquet(file_path).select(pl.count()).collect().item()


class JSONReader(BaseReader):
    def read(self, file_path: Path) -> pl.LazyFrame:
        # Auto-detect JSON lines vs single JSON
        return pl.scan_ndjson(file_path)

    def count_rows(self, file_path: Path) -> int:
        with open(file_path, "rb") as f:
            return sum(1 for _ in f)


class ExcelReader(BaseReader):
    def read(self, file_path: Path) -> pl.LazyFrame:
        # Polars doesn't lazy-read Excel, use pandas intermediate
        df = pd.read_excel(file_path, engine="openpyxl")
        return pl.from_pandas(df).lazy()

    def count_rows(self, file_path: Path) -> int:
        # Read only shape info
        df = pd.read_excel(file_path, engine="openpyxl", nrows=0)
        return len(pd.read_excel(file_path, engine="openpyxl"))


class ReaderFactory:
    """Factory to get appropriate reader."""

    _readers: Dict[FileType, BaseReader] = {
        FileType.CSV: CSVReader(),
        FileType.PARQUET: ParquetReader(),
        FileType.JSON: JSONReader(),
        FileType.EXCEL: ExcelReader(),
    }

    @classmethod
    def get_reader(cls, file_type: FileType) -> BaseReader:
        if file_type not in cls._readers:
            raise ValueError(f"No reader for type: {file_type}")
        return cls._readers[file_type]


# ── Writers (Strategy Pattern) ──────────────────────────────────────────────
class BaseWriter:
    """Abstract base for output writers."""

    def write(self, lf: pl.LazyFrame, output_path: Path, **kwargs) -> Path:
        raise NotImplementedError


class ParquetWriter(BaseWriter):
    def write(self, lf: pl.LazyFrame, output_path: Path, **kwargs) -> Path:
        lf.sink_parquet(output_path)
        return output_path


class CSVWriter(BaseWriter):
    def write(self, lf: pl.LazyFrame, output_path: Path, **kwargs) -> Path:
        lf.sink_csv(output_path)
        return output_path


class SQLWriter(BaseWriter):
    """Write to SQL via DuckDB."""

    def __init__(self, connection_string: Optional[str] = None):
        self.conn_str = connection_string or ":memory:"

    def write(self, lf: pl.LazyFrame, output_path: Path, **kwargs) -> Path:
        # Convert to DuckDB table
        df = lf.collect()
        conn = duckdb.connect(
            str(output_path) if str(output_path).endswith(".duckdb") else ":memory:"
        )
        table_name = kwargs.get("table_name", "data")
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        conn.close()
        return output_path


class JSONWriter(BaseWriter):
    def write(self, lf: pl.LazyFrame, output_path: Path, **kwargs) -> Path:
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


# ── Core Converter ──────────────────────────────────────────────────────────
class FileConverter:
    """
    Main conversion engine. Handles single file or group conversion.
    """

    def __init__(self, output_dir: Path = OUTPUT_DIR):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)

    @logger.catch(reraise=True)
    def convert_file(
        self,
        file_path: Path,
        output_format: OutputFormat,
        output_name: Optional[str] = None,
        **writer_kwargs,
    ) -> ConversionResult:
        """
        Convert a single file to target format.
        """
        start = time.perf_counter()
        file_type = FileTypeDetector.detect(file_path)

        try:
            # Get reader and writer
            reader = ReaderFactory.get_reader(file_type)
            writer = WriterFactory.get_writer(output_format)

            # Read
            logger.info(f"[{file_path.name}] Reading as {file_type.name}...")
            lf = reader.read(file_path)

            # Determine output path
            out_name = output_name or file_path.stem
            out_path = self.output_dir / f"{out_name}.{output_format.value}"

            # Write
            logger.info(f"[{file_path.name}] Writing to {output_format.name}...")
            result_path = writer.write(lf, out_path, **writer_kwargs)

            # Stats
            duration = time.perf_counter() - start
            try:
                records = reader.count_rows(file_path)
            except Exception:
                records = -1

            file_size_mb = os.path.getsize(file_path) / (1024**2)
            out_size_mb = os.path.getsize(result_path) / (1024**2)

            logger.success(
                f"[{file_path.name}] ✓ Converted in {duration:.2f}s | "
                f"Rows: {records:,} | "
                f"In: {file_size_mb:.2f}MB → Out: {out_size_mb:.2f}MB"
            )

            return ConversionResult(
                source=file_path,
                output=result_path,
                success=True,
                duration=duration,
                records=records,
            )

        except Exception as e:
            duration = time.perf_counter() - start
            logger.error(f"[{file_path.name}] ✗ Failed: {str(e)}")
            return ConversionResult(
                source=file_path,
                output=None,
                success=False,
                duration=duration,
                records=0,
                error=str(e),
            )

    def convert_group(
        self,
        group: FileGroup,
        output_format: OutputFormat,
        merge: bool = True,
        **writer_kwargs,
    ) -> List[ConversionResult]:
        """
        Convert all files in a group.
        If merge=True, combine into single output (for same-schema files).
        """
        results = []

        if merge and len(group.files) > 1:
            # Merge mode: combine all files
            logger.info(
                f"[Group: {group.base_name}] Merging {len(group.files)} files..."
            )
            start = time.perf_counter()

            try:
                reader = ReaderFactory.get_reader(group.detected_type)

                # Lazy union all files
                lfs = [reader.read(f) for f in group.files]
                combined = pl.concat(lfs, how="diagonal_relaxed")

                writer = WriterFactory.get_writer(output_format)
                out_path = self.output_dir / f"{group.base_name}.{output_format.value}"
                result_path = writer.write(combined, out_path, **writer_kwargs)

                duration = time.perf_counter() - start
                total_rows = sum(
                    ReaderFactory.get_reader(group.detected_type).count_rows(f)
                    for f in group.files
                )

                logger.success(
                    f"[Group: {group.base_name}] ✓ Merged {len(group.files)} files "
                    f"in {duration:.2f}s | Total rows: {total_rows:,}"
                )

                results.append(
                    ConversionResult(
                        source=group.files[0],  # representative
                        output=result_path,
                        success=True,
                        duration=duration,
                        records=total_rows,
                    )
                )

            except Exception as e:
                logger.error(f"[Group: {group.base_name}] ✗ Merge failed: {e}")
                # Fallback: convert individually
                logger.info(
                    f"[Group: {group.base_name}] Falling back to individual conversion..."
                )
                for f in group.files:
                    results.append(self.convert_file(f, output_format))
        else:
            # Individual conversion
            for f in group.files:
                results.append(self.convert_file(f, output_format))

        return results


# ── Parallel Orchestrator ─────────────────────────────────────────────────────
class ParallelConverter:
    """
    High-level orchestrator with parallel processing.
    """

    def __init__(
        self,
        input_dir: Path,
        output_dir: Path = OUTPUT_DIR,
        max_workers: Optional[int] = None,
    ):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.converter = FileConverter(output_dir)
        self.max_workers = max_workers or max(1, os.cpu_count() // 2)
        self.grouper = FileGrouper()

    def scan_input(self, pattern: str = "*") -> List[Path]:
        """Scan input directory for supported files."""
        files = []
        for ext in FileTypeDetector.EXTENSION_MAP.keys():
            files.extend(self.input_dir.glob(f"*{ext}"))
        logger.info(f"Found {len(files)} supported files in {self.input_dir}")
        return files

    def run(
        self,
        output_format: OutputFormat = OutputFormat.PARQUET,
        group_by_name: bool = True,
        merge_groups: bool = True,
        **writer_kwargs,
    ) -> Dict[str, List[ConversionResult]]:
        """
        Run full conversion pipeline.

        Args:
            output_format: Target format
            group_by_name: Whether to group files by base name
            merge_groups: Whether to merge grouped files
            **writer_kwargs: Extra args for writers (e.g., table_name for SQL)
        """
        # Scan
        files = self.scan_input()
        if not files:
            logger.warning("No files found to convert!")
            return {}

        # Group
        if group_by_name:
            groups = self.grouper.group_files(files)
            logger.info(f"Grouped into {len(groups)} groups")
        else:
            # Each file is its own group
            groups = {
                f.stem: FileGroup(f.stem, [f], FileTypeDetector.detect(f))
                for f in files
            }

        # Convert
        all_results: Dict[str, List[ConversionResult]] = {}

        # Sequential for groups (to avoid deadlocks with DuckDB/Polars)
        # Parallel within groups if not merging
        for group_name, group in groups.items():
            logger.info(f"Processing group: {group_name}")
            results = self.converter.convert_group(
                group, output_format, merge=merge_groups, **writer_kwargs
            )
            all_results[group_name] = results

        # Summary
        total_files = sum(len(r) for r in all_results.values())
        success = sum(
            1 for results in all_results.values() for r in results if r.success
        )
        failed = total_files - success

        logger.info("=" * 60)
        logger.info(f"CONVERSION COMPLETE")
        logger.info(f"Total groups: {len(groups)}")
        logger.info(f"Total files processed: {total_files}")
        logger.info(f"Success: {success} | Failed: {failed}")
        logger.info("=" * 60)

        return all_results


# ── Usage Example ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Configure logging
    import datetime

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    logger.add(f"./logs/conversion_{today}.log", rotation="100 MB", level="INFO")

    # Initialize
    pipeline = ParallelConverter(
        input_dir=Path(r"C:\Users\rian.pham\Downloads\OneDrive_1_6-5-2026"),
        max_workers=4,
    )

    # Run with different configurations
    # 1. Convert to Parquet, group and merge
    results = pipeline.run(
        output_format=OutputFormat.PARQUET, group_by_name=True, merge_groups=True
    )

    # 2. Convert to CSV, no grouping
    # results = pipeline.run(
    #     output_format=OutputFormat.CSV,
    #     group_by_name=False
    # )

    # 3. Convert to DuckDB SQL
    # results = pipeline.run(
    #     output_format=OutputFormat.DUCKDB,
    #     group_by_name=True,
    #     merge_groups=True,
    #     table_name="customer_tracking"
    # )
