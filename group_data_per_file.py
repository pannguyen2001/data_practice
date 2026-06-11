"""
Detect file have same main info, then concat all same name sheet to 1 data and write to parquet file
"""

# %%
# ====================
# Step 1 — Read one sheet from one file (worker unit)
# ====================
import re
from collections import defaultdict
from pathlib import Path
from textwrap import indent

from loguru import logger

import polars as pl


def extract_main_name(file_path: str | Path) -> str:
    """
    Extracts main template name from filenames like:
      'Data Template T01_Application_C2R2_21_Apr_2026_TMS.xlsx'
      'Data Template T01_Application_C2R2_21_Apr_2026_2_TMS.xlsx'
      'Data Template M01_User Information_C2R2_16_Apr_2026_TMS.xlsx'
      'Data Template T01_Application_C2R2_17_Apr_2026_LXP (1).xlsx'
      'Data Template T01_Application_C2R2_17_Apr_2026_LXP new.xlsx'

    Pattern: 'Data Template <XXX>_<MainName>_<CxRy>_<...anything...>'
    Returns:  'Application', 'User Information', etc.
    """
    stem = Path(file_path).stem  # remove .xlsx

    # Strip trailing duplicate suffixes BEFORE extracting name:
    # ' (1)', ' (2)', ' 1', ' new', ' copy', ' v2', ' final', etc.
    stem = re.sub(r"\s*\(\d+\)\s*$", "", stem).strip()
    stem = re.sub(
        r"\s+(new|copy|v\d+|final|revised|updated|\d+)$", "", stem, flags=re.IGNORECASE
    ).strip()

    # Core pattern:
    # 'Data Template ' + <XXX (no underscore)> + '_' + <MainName> + '_' + <CxRy pattern>
    # CxRy pattern: C followed by digits, R followed by digits (e.g. C2R2, C3R1)
    match = re.search(
        r"Data Template\s+[^_]+_(.+?)_C\d+R\d+_",
        stem,
        re.IGNORECASE,
    )
    if match:
        return match.group(1).strip()  # e.g. 'Application', 'User Information'

    # Fallback: take segment between first and second underscore after 'Data Template '
    after_prefix = re.sub(r"^Data Template\s+", "", stem, flags=re.IGNORECASE)
    parts = after_prefix.split("_")
    if len(parts) >= 2:
        return parts[1].strip()

    return stem


def group_files_by_main_name(folder_path: str) -> dict[str, list[Path]]:
    """
    Scans folder and groups .xlsx files by their main template name.
    Returns: {'Application': [Path(...), Path(...)], 'Timetabling': [...]}
    """
    folder = Path(folder_path)
    files = list(folder.glob("*.xlsx"))

    groups: dict[str, list[Path]] = defaultdict(list)
    for f in files:
        main_name = extract_main_name(f)
        groups[main_name].append(f)

    # Sort files within each group for deterministic order
    for name in groups:
        groups[name].sort()

    return dict(groups)


def read_sheet_from_file(
    file_path: Path,
    sheet_name: str,
) -> pl.DataFrame:
    """Smallest unit of work: one sheet from one file."""
    df = pl.read_excel(
        file_path,
        sheet_name=sheet_name,
        engine="calamine",
    )
    return df.with_columns(
        [
            pl.lit(file_path.name).alias("_source_file"),
        ]
    )


# %%
# ====================
# Step 2 — Get sheet names from the first file in the group
# ====================
def get_sheet_names(file_path: Path) -> list[str]:
    """Read sheet names without loading any data."""
    from python_calamine import CalamineWorkbook

    wb = CalamineWorkbook.from_path(str(file_path))
    sheet_names = [sheet for sheet in wb.sheet_names if sheet != "Config Data"]
    return sheet_names


# %%
# ====================
# Step 3 — Process one group: all sheets, all files, parallel by sheet
# ====================
from pathlib import Path

import polars as pl


def write_sheet_parquet(
    df: pl.DataFrame,
    output_dir: Path,
    group_name: str,
    sheet_name: str,
) -> Path:
    """Write a single sheet DataFrame to parquet. Returns the output path."""
    group_dir = output_dir / group_name
    group_dir.mkdir(parents=True, exist_ok=True)

    # Sanitize sheet name for filesystem (some sheets have slashes, spaces, etc.)
    safe_sheet_name = sheet_name.replace("/", "_").replace("\\", "_").strip()
    out_path = group_dir / f"{safe_sheet_name}.parquet"

    df.write_parquet(
        out_path,
        compression="snappy",  # fast read/write, good ratio
        statistics=True,  # enables predicate pushdown when reading back
        row_group_size=100_000,  # tune based on your row counts
    )
    logger.info(
        f"Written: {out_path} ({df.shape[0]: ,} rows, {out_path.stat().st_size / 1e6:.2f} MB)"
    )
    return out_path


from concurrent.futures import ThreadPoolExecutor, as_completed


def process_group(
    main_name: str,
    file_paths: list[Path],
    output_dir: Path,  # ← add this
    max_workers: int = 6,
) -> tuple[str, dict[str, Path]]:  # ← now returns paths, not DataFrames
    """
    Reads, concats, and writes parquet per sheet.
    Returns: ('Application', {'User': Path(...), 'Course': Path(...), ...})
    """
    sheet_names = get_sheet_names(file_paths[0])
    tasks = [(sheet, fp) for sheet in sheet_names for fp in file_paths]
    sheet_frames: dict[str, list[tuple[str, pl.DataFrame]]] = {
        s: [] for s in sheet_names
    }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(read_sheet_from_file, fp, sheet): (sheet, fp)
            for sheet, fp in tasks
        }
        for future in as_completed(future_to_task):
            sheet, fp = future_to_task[future]
            try:
                df = future.result()
                sheet_frames[sheet].append((fp.name, df))
            except Exception as e:
                logger.error(f"[{main_name}] Failed {fp.name} / sheet '{sheet}': {e}")

    sheet_paths: dict[str, Path] = {}
    for sheet, frames in sheet_frames.items():
        if not frames:
            logger.warning(f"[{main_name}][{sheet}] No frames — skipping.")
            continue

        frames.sort(key=lambda x: x[0])
        try:
            df_concat = pl.concat([f for _, f in frames], how="diagonal_relaxed")
            if df_concat.is_empty():
                logger.info(f"No data. Skip {sheet}")
            else:
                out_path = write_sheet_parquet(df_concat, output_dir, main_name, sheet)
                sheet_paths[sheet] = out_path
        except Exception as e:
            logger.error(f"[{main_name}][{sheet}] Failed: {e}")

    return main_name, sheet_paths


# ============================
# Step 4 — Orchestrate all groups with ProcessPoolExecutor
# ============================
import os
from concurrent.futures import ProcessPoolExecutor


def load_all_groups(
    folder_path: str,
    output_dir: str = "output",
    max_group_workers: int | None = None,
) -> dict[str, dict[str, Path]]:
    """
    Returns paths to written parquet files:
    {
      'Application': {'User': Path('output/Application/User.parquet'), ...},
      'Timetabling': {'Slot': Path('output/Timetabling/Slot.parquet'), ...},
    }
    """
    groups = group_files_by_main_name(folder_path)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    n_workers = max_group_workers or max(1, min(os.cpu_count() - 1, len(groups)))
    results: dict[str, dict[str, Path]] = {}

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = {
            executor.submit(process_group, name, paths, out): name
            for name, paths in groups.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                group_name, sheet_paths = future.result()
                results[group_name] = sheet_paths
                logger.info(
                    f"[{group_name}] Parquet files: {list(sheet_paths.values())}"
                )
            except Exception as e:
                logger.exception(f"[{name}] Failed: {e}")

    return results


# ==============================
# Step 4 — Read back with DuckDB or Polars
# ==============================
import duckdb


def register_parquet_in_duckdb(
    all_paths: dict[str, dict[str, Path]],
) -> duckdb.DuckDBPyConnection:
    """Register parquet files as DuckDB views — fully lazy, no data loaded."""
    con = duckdb.connect()
    for group_name, sheets in all_paths.items():
        for sheet_name, parquet_path in sheets.items():
            view_name = f"{group_name}_{sheet_name}"
            # DuckDB reads parquet lazily — only loads what the query needs
            con.execute(f"CREATE VIEW {view_name} AS SELECT * FROM '{parquet_path}'")
            logger.info(f"DuckDB view: '{view_name}' → {parquet_path}")
    return con


# Or read back as Polars LazyFrame for pipeline chaining:
def read_back_as_lazy(
    all_paths: dict[str, dict[str, Path]],
) -> dict[str, dict[str, pl.LazyFrame]]:
    return {
        group: {sheet: pl.scan_parquet(path) for sheet, path in sheets.items()}
        for group, sheets in all_paths.items()
    }


# ==============================
# Usage
# ==============================
if __name__ == "__main__":
    import multiprocessing as mp

    mp.freeze_support()

    # Process + write parquet
    # Process + write parquet
    all_paths = load_all_groups(
        folder_path=r"C:\Users\rian.pham\Downloads\OneDrive_1_6-2-2026",
        output_dir="output_OneDrive_1_6-2-2026",
    )

    logger.info(all_paths)

    # Option A — DuckDB on parquet files (lazy, memory efficient)
    # con = register_parquet_in_duckdb(all_paths)
    # result = con.execute("""
    #     SELECT _source_file, COUNT(*) as rows
    #     FROM Application_User
    #     GROUP BY _source_file
    # """).pl()

    # Option B — Polars LazyFrame pipeline
    # lazy_groups = read_back_as_lazy(all_paths)
    # logger.info(lazy_groups["Application"]["Application"].collect_schema())
