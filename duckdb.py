# %%
import duckdb

import polars as pl

# %%
file_path: str = r"C:\_My_job\_Code\_try python\output_OneDrive_1_6-2-2026\Finance Process\HonorariumRecord.parquet"
con = duckdb.connect()

df = con.sql(fr"""
SELECT * FROM read_parquet('{file_path}')
    """)
df
# %%
df.columns

# %%
con.sql(
    f"""
    CREATE OR REPLACE VIEW Honorarium AS
        SELECT * FROM read_parquet('{file_path}');
    """
)


# %%
con.sql("""
WITH hono_batch_sum_amount AS (
    SELECT "Honorarium batch", SUM("Amount") AS total_amount
    FROM Honorarium
    GROUP BY "Honorarium batch"
    ORDER BY total_amount DESC
)
SELECT *
FROM hono_batch_sum_amount
    """)

# %%
con.sql("""
SELECT COUNT(*) AS total_rows
FROM Honorarium
""")

# %%
pl.Config(set_tbl_cols=-1, set_tbl_rows=-1)
con.sql("""
    SELECT *
    FROM Honorarium
    FETCH FIRST 1 ROWS ONLY
""").pl().transpose(include_header=True, header_name="Column_Name")

# %%


# %%
# Count invalid value
con.sql("""
    SELECT
        count(*) FILTER (WHERE "Honorarium record ID" IS NULL) AS missing_ids,
        count(*) FILTER (WHERE Amount < 0 ) AS negative_amount,
        count(*) FILTER (WHERE "Created On" < "Modified On" ) AS invalid_datetime_logic,
        count(*) FILTER (
            WHERE strptime("Created On", '%d/%m/%Y %H:%M:%S') > current_timestamp
            OR strptime("Modified On", '%d/%m/%Y %H:%M:%S') > current_timestamp
        ) AS invalid_created_on,
        count(*) FILTER(
            try_strptime("Created On", '%d/%m/%Y %H:%M:%S') IS NULL
        )  AS is_valid_datetime_format,
        count(*) FILTER (WHERE "Type" IS NULL AND CourseIntakeId IS NOT NULL ) AS missing_type
    FROM Honorarium;
""")

# %%
# Window function
con.sql(
    """
SELECT
    row_number() OVER () as row_num,
    "Honorarium record ID",
    "Honorarium batch",
    "Created On",
    SUM("Amount") OVER(PARTITION BY "Honorarium batch") as row_number_batch_amount
FROM Honorarium
ORDER BY row_number_batch_amount DESC;
    """
)

# %%
con.sql("""
SELECT
    "Honorarium batch",
    SUM(Amount) AS total_batch_amount
FROM Honorarium
GROUP BY "Honorarium batch"
HAVING total_batch_amount > 1000
ORDER BY "Honorarium batch" DESC
""")


# %%
con.sql("""
WITH total_batch_amount_larger_than_1000 AS (
    SELECT
        "Honorarium batch",
        SUM(Amount) AS total_batch_amount
    FROM Honorarium
    GROUP BY "Honorarium batch"
    HAVING total_batch_amount > 1000
    ORDER BY "Honorarium batch" DESC
)
SELECT COUNT(*)
FROM total_batch_amount_larger_than_1000
""")

# %%
# %%
con.sql("""
SELECT
    "Honorarium batch",
    SUM(Amount) AS total_batch_amount
FROM Honorarium
GROUP BY "Honorarium batch"
ORDER BY total_batch_amount DESC
LIMIT 1
""")

# %%
con.sql("""
WITH total_batch_amount_tb AS (
    SELECT
        "Honorarium batch",
        SUM(Amount) AS total_batch_amount
    FROM Honorarium
    GROUP BY "Honorarium batch"
)
SELECT *
FROM total_batch_amount_tb
WHERE total_batch_amount = (SELECT MAX(total_batch_amount) FROM total_batch_amount_tb)
""")
