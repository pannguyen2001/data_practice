# %%
import datetime
import os
import sys
from pathlib import Path
from string import Template

import pandas as pd
import pytz
from dotenv import load_dotenv
from loguru import logger

logger.remove()
logger.level(name="DEBUG", color="<blue>", icon="🔍 ")
logger.level(name="INFO", color="<green>", icon="💡 ")
logger.level(name="SUCCESS", color="<cyan>", icon="😀")
logger.level(name="WARNING", color="<yellow>", icon="❕")
logger.level(name="ERROR", color="<red>", icon="❌")
logger.level(name="CRITICAL", color="<white>", icon="🚫")
logger.add(
    sys.stdout,
    colorize=True,
    format="<bold><level>{level.icon:<2}</level><level>[{level}]</level>[<green>{time:YYYY-MM-DD HH:mm:ss}</green>][<cyan>{name}:{function}:{line}</cyan>]</bold> <level>{message}</level>",
    # level="TRACE" # default is DEBUG
)

write_successfully_template = Template("""Write data to excel file successfully.
Detail info:
    Sheet name: ${sheet_name}.
    File: ${file_path}.
    Mode: ${mode_flag}.
    If sheet exists: ${if_sheet_exists}.""")


@logger.catch
def write_data(
    df: pd.DataFrame,
    file_path: str,
    sheet_name: str = "Sheet1",
    index: bool = False,
    mode: str = "replace",
    *args,
    **kwargs,
) -> None:
    logger.info(
        f"[{write_data.__name__}] Write data to excel sheet: {sheet_name}, file: {file_path}"
    )
    mode_flag = "w" if not os.path.exists(file_path) else "a"
    if_sheet_exists = "replace" if mode == "replace" else "overlay"
    config = {"engine": kwargs.get("engine") or "openpyxl", "mode": mode_flag, **kwargs}
    if mode_flag == "a":
        config["if_sheet_exists"] = if_sheet_exists
    with pd.ExcelWriter(file_path, **config) as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=index, *args, **kwargs)
    logger.success(
        write_successfully_template.safe_substitute(
            sheet_name=sheet_name,
            file_path=file_path,
            mode_flag=mode_flag,
            if_sheet_exists=if_sheet_exists,
        )
    )


@logger.catch
def write_to_csv(
    df: pd.DataFrame,
    file_path: str,
    index: bool = False,
    *args,
    **kwargs,
) -> None:
    df.to_csv(file_path, index=index, *args, **kwargs)
    logger.success(f"Write data to: {file_path}")


folder_path = Path(
    r"C:\Users\rian.pham\Downloads\Pre_ValidationResult_20260523112452\Pre_ValidationResult_20260523112452\Detail"
)
report_type: str = "Pre"
import datetime

date_today = datetime.datetime.strftime(
    datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=7))),
    format="%Y-%m-%d",
)
report_folder_path: str = f"reports/summary_{date_today}"
import os

if not os.path.exists(report_folder_path):
    os.makedirs(report_folder_path)
files: list = []
index: int = 1

# Iterates through all items in the directory
for file_path in folder_path.iterdir():
    # Check if it's a file (not a folder)
    logger.info(file_path)
    file_path = str(file_path)
    sheet_names = pd.ExcelFile(file_path, engine="calamine").sheet_names
    for sheet_name in sheet_names:
        if sheet_name == "ErrorCodeInstruction":
            continue
        logger.info(sheet_name)
        file_name: str = file_path.split("\\")[-1]
        df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
        df = df.sort_values(
            by=[df.columns[0], df.columns[1], df.columns[2]]
        ).reset_index(drop=True)
        df[df.columns[0]] = (
            df.columns[0]
            + ": "
            + df[df.columns[0]].astype(str)
            + " - "
            + df.columns[1]
            + ": "
            + df[df.columns[1]].astype(str)
            + " - "
            + df.columns[2]
            + ": "
            + df[df.columns[2]].astype(str)
            + "(Report index: "  # In future, read origin data and get value origin in report data, and sort by origin data
            + (df.index + 2).astype(str)
            + ")"
        )
        df["Comment"] = df["Comment"].astype(str).map(lambda x: x.split("\n"))
        df = df.explode("Comment", ignore_index=True)
        if report_type == "Pre":
            df["Comment"] = df["Comment"].astype(str).map(lambda x: x.split("_"))
            df["Error type"] = df["Comment"].map(lambda x: x[1] if len(x) > 0 else "")
            df["Message"] = df["Comment"].map(lambda x: x[2] if len(x) > 0 else "")
        else:
            df["Error type"] = ""
            df["Message"] = df["Comment"]
        df_summary = (
            df.groupby(["Message", "Migration Status", "Error type"])
            .agg({df.columns[0]: list})
            .reset_index()
        )
        df_summary["Amount"] = df_summary[df.columns[0]].map(lambda x: len(x))
        df_summary = df_summary.sort_values(by=["Message"]).reset_index(drop=True)
        df_summary[df.columns[0]] = df_summary[df.columns[0]].map(
            lambda x: [i for i in x if pd.notna(i)]
        )
        # logger.info(df_summary[df.columns[0]])
        df_summary[df.columns[0]] = df_summary[df.columns[0]].map(
            lambda x: "\n".join(x[:5])
        )
        df_summary = df_summary[
            ["Migration Status", "Error type", "Message", "Amount", df.columns[0]]
        ]
        if report_type == "Pre":
            df_summary = df_summary.drop(columns=["Migration Status"])
        else:
            df_summary = df_summary.drop(columns=["Error type"])
        # print(df_summary)
        file_name: str = file_path.replace("\\", "/").split("/")[-1]
        # summary_file_path: str = f"{report_folder_path}/{file_name}"
        # write_data(
        #     df_summary, file_path=summary_file_path, sheet_name=sheet_name, index=False
        # )
        summary_file_path: str = f"{report_folder_path}/{index}_{sheet_name}.csv"
        index += 1
        write_to_csv(df_summary, summary_file_path)
