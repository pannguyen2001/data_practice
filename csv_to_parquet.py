# %%
import os
import time
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat
from pathlib import Path

import duckdb
from loguru import logger

import polars as pl

# ── Step 1: Write phase (unchanged, but cleaner) ─────────────────────────────

OUTPUT_DIR = Path("parquet_output")
OUTPUT_DIR.mkdir(exist_ok=True)

folder_path: str = r"C:\Users\rian.pham\Downloads\19th_customer_tracking_log"
files = os.listdir(folder_path)
files = [f for f in files if f.endswith(".csv")]
files = [f"{folder_path}/{f}".replace("\\", "/") for f in files]
# logger.info(files)


@logger.catch
def csv_to_parquet(file_path: str) -> str:
    """Convert Excel sheet to Parquet using Polars natively."""
    logger.info(
        f"Convert excel to parquet: {file_path}. File size: {os.path.getsize(file_path) // 1024**2: .2f} (MB)"
    )
    file_name: str = file_path.split("/")[-1].split(".")[0]
    parquet_path = f"{OUTPUT_DIR}/{file_name}.parquet"
    start = time.perf_counter()
    pl.scan_csv(file_path).sink_parquet(parquet_path)
    end = time.perf_counter()
    logger.success(
        "Complete transfer excel to parquet. "
        f"Sheet_name: {file_name}. "
        f"Time consuming: {end - start: .2f}(s). "
    )
    return parquet_path


# ── Step 2: Parallel write ────────────────────────────────────────────────────


def main(files: list[str]) -> None:

    max_workers = min(
        os.cpu_count() // 2,
        len(files),
    )
    logger.info(
        f"CPU thread: {os.cpu_count()}. Num of data: {len(files)}. Max workers will be used: {max_workers}."
    )

    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        paths = list(
            pool.map(
                csv_to_parquet,
                files,
            )
        )
        logger.info(f"All paths: {paths}")


# %%
# if __name__ == "__main__":
# main(files)


# %%
import duckdb
from loguru import logger

# con = duckdb.connect()
with duckdb.connect() as con:
    file_name: str = (
        r"C:\_My_job\_Code\_try python\parquet_output\01-log-tracking-001.parquet"
    )
    read_tracking_query = f"""
    SELECT * FROM read_parquet('{file_name}')"""
    tracking = con.sql(read_tracking_query)
    logger.info(tracking.shape)

# %%
logger.info(tracking.columns)

# %%
con = duckdb.connect()
file_name: str = (
    r"C:\_My_job\_Code\_try python\parquet_output\01-log-tracking-001.parquet"
)
read_tracking_query = f"""
SELECT * FROM read_parquet('{file_name}')"""
tracking = con.sql(read_tracking_query)

# %%
con.sql("SUMMARIZE tracking").show()

# %%
con.sql("SELECT COUNT(DISTINCT event_type) FROM tracking").fetchone()


# %%
con.table("tracking").aggregate("COUNT (DISTINCT event_type) AS num_of_event_type")

# %%
tracking.create_view("tracking")
con.sql(
    """
    SELECT
        event_type, COUNT (event_type) AS num_of_event_type
    FROM tracking
    GROUP BY event_type
    ORDER BY num_of_event_type DESC
    """,
)
