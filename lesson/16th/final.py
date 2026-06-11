# # %%
# # ==============================
# # Download data
# # ==============================
# import os

# import gdown

# folder_path: str = "./data/staging/19th"
# gg_folder_url: str = (
#     "https://drive.google.com/drive/folders/1nSTzQD2ezMOTgsy5fF9eqpjaDmdwyLPa"
# )

# if not os.path.exists(folder_path):
#     os.makedirs(folder_path)

# gdown.download_folder(url=gg_folder_url, output=folder_path, quiet=False)

# ==============================
# Transfer from other file type to parquet/sql/csv (if need)
# ==============================


# %%
# ==============================
# Get all files in origin folder
# ==============================
import os
from pathlib import Path

# Replace with your actual directory path
origin_folder: str = r"/home/user/dapractice/data/staging/16th"
folder_path = Path(origin_folder)

# Get all files in the immediate directory
files = [f"{origin_folder}/{str(f.name)}" for f in folder_path.iterdir() if f.is_file()]
print(files)


# %%
# ==============================
# Read data
# Curently, using polars with read_csv(encoding="utf8-lossy") to read non utf8 chars
# ==============================
import polars as pl

des_folder: str = r"/home/user/dapractice/data/raw/16th"
if not os.path.exists(des_folder):
    os.makedirs(des_folder)

for file_path in files:
    file_name: str = file_path.replace("\\", "/").split("/")[-1].split(".")[0]
    scanned_listing = pl.scan_csv(
        file_path,
        has_header=True,
        # infer_schema_length=0,
        truncate_ragged_lines=True,
        encoding="utf8-lossy",
    )

    df = scanned_listing.collect()
    df.write_parquet(f"{des_folder}/{file_name}.parquet")

    # if "dictionary" not in file_name:
    #     schema = df.schema
    #     df_schema = pl.DataFrame(
    # {"column_name": list(schema.keys()), "data_type": [str(dt) for dt in schema.values()]})

    #     df_schema.write_parquet(f"{des_folder}/{file_name}_schema.parquet")

    print(f"Complete transfer file: {file_name}")


# %%
