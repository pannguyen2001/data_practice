import time
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat
from pathlib import Path

import duckdb
from loguru import logger

import polars as pl

# ── Step 1: Write phase (unchanged, but cleaner) ─────────────────────────────

SHEET_NAMES = [
    "BillingAdvice",
    "BillingAdviceInstalment",
    "BillingAdviceBillingCharges",
    "BillingAdviceSubsidy",
    "BillingAdviceMiscOnly",
    "BillingAdvicePayment",
    "BillingAdviceInvoice",
]
OUTPUT_DIR = Path("parquet_output")
OUTPUT_DIR.mkdir(exist_ok=True)
file_path: str = (
    r"C:\Users\rian.pham\Downloads\Compare_Finance Process_Original report.xlsx"
)


@logger.catch
def excel_to_parquet(file_path: str, sheet_name: str) -> str:
    """Convert Excel sheet to Parquet using Polars natively."""
    logger.info(f"Convert excel to parquet: {sheet_name}")
    start = time.perf_counter()
    df = pl.read_excel(file_path, sheet_name=sheet_name)
    parquet_path = f"{OUTPUT_DIR}/{sheet_name}.parquet"
    df.write_parquet(parquet_path)
    end = time.perf_counter()
    logger.success(
        "Complete transfer excel to parquet. "
        f"Sheet_name: {sheet_name}. "
        f"Time consuming: {end - start: .2f}(s). "
        f"Data size: {df.estimated_size(unit='mb'): .2f}(MB), "
        f"{df.height} rows, {df.width} columns."
    )
    return parquet_path


# ── Step 2: Parallel write ────────────────────────────────────────────────────
import os
from itertools import repeat


def main():

    max_workers = min(
        os.cpu_count() // 2,
        len(SHEET_NAMES),
    )
    logger.info(
        f"CPU thread: {os.cpu_count()}. Num of data: {len(SHEET_NAMES)}. Max workers will be used: {max_workers}."
    )

    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        paths = list(
            pool.map(
                excel_to_parquet,
                repeat(file_path),
                SHEET_NAMES,
            )
        )

    logger.info(paths)


if __name__ == "__main__":
    main()
