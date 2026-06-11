#!/usr/bin/env python3

import os
import sys
import traceback
from functools import wraps
from string import Template
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger

from common import *

error_template = Template("""[${funct_name}] has error:
${error}""")

logger.remove()

logger.add(
    sys.stdout,
    colorize=True,
    format="<level>[{level}]</level>[<green>{time:YYYY-MM-DD HH:mm:ss}</green>][<cyan>{name}:{function}:{line}</cyan>] <level>{message}</level>",
    # level="TRACE" # default is DEBUG
)
logger.add(
    "logger.log",
    colorize=False,
    format="<level>[{level}]</level>[<green>{time:YYYY-MM-DD HH:mm:ss}</green>][<cyan>{name}:{function}:{line}</cyan>] <level>{message}</level>",
    # level="TRACE" # default is DEBUG
)

# eteam_ui_data_path: str = r"C:\_IAL finance migration - Eteam\_Data\eteam_export_data\25_12_16"

# from os import walk

# f = []
# for (dirpath, dirnames, filenames) in walk(eteam_ui_data_path):
#     f.extend(filenames)
#     break
# logger.success(f"Files in {eteam_ui_data_path} : {f}")

# course_fee_scheme_all_file_names: List = [file for file in f if "eteam_course_fee_scheme_all" in file]
# course_fee_scheme_all_file_names


# eteam_ui_course_fee_scheme_file_path: list = [os.path.join(eteam_ui_data_path, file).replace("\\", "/") for file in course_fee_scheme_all_file_names]
# logger.success(eteam_ui_course_fee_scheme_file_path)

# df_course_fee_scheme: pd.DataFrame = pd.DataFrame()
# for file in eteam_ui_course_fee_scheme_file_path:
#     df_course_fee_scheme = pd.concat([df_course_fee_scheme, pd.read_excel(file, engine="openpyxl")])
# logger.info(df_course_fee_scheme.head())

# df_course_fee_scheme.to_excel(f"{eteam_ui_data_path}/eteam_course_fee_all_final.xlsx", index=False)


# ======================== Summarize report =====================================
# course_fee_pre_report_path: str = r"C:\_IAL finance migration - Eteam\_Report\Pre_ValidationResult_20251218105948\Details\Course Setup_report.xlsx"
# course_fee_sheeet_name: str = "CourseFeeSetup"

# df_course_feer_setup_report: pd.DataFrame = pd.read_excel(course_fee_pre_report_path, sheet_name=course_fee_sheeet_name)

# df_course_fee_setup_split: pd.DataFrame = df_course_feer_setup_report[[
#     'CourseUniqueId',
#     "Migration Status",
#     "Comment"
#     ]]

# df_course_fee_setup_split["Comment"] = df_course_fee_setup_split["Comment"].str.strip().astype(str).str.split(r"\n")
# df_course_fee_setup_split["Comment"] = df_course_fee_setup_split["Comment"].map(lambda x: [i.strip() for i in x])
# df_course_fee_setup_split = df_course_fee_setup_split.explode("Comment")
# df_course_fee_setup_split["final_id"] = df_course_fee_setup_split["CourseUniqueId"] + " - " + (df_course_fee_setup_split.index + 2).astype(str)
# df_course_fee_setup_split = df_course_fee_setup_split.groupby("Comment").agg({
#     "final_id": list,
#     # "Migration Status": list,
# })
# df_course_fee_setup_split["amount"] = df_course_fee_setup_split["final_id"].map(lambda x: len(x))
# df_course_fee_setup_split["final_id"] = df_course_fee_setup_split["final_id"].map(lambda x: "\n".join(x))
# df_course_fee_setup_split = df_course_fee_setup_split.reset_index()
# df_course_fee_setup_split.to_excel("course_fee_setup_pre_tool_summarize.xlsx", index= False)
# print(df_course_fee_setup_split)


# =============== Compare import data vs Source compare===============
# Finance setup
data_template_path: str = r"C:\_IAL finance migration - Eteam\_Report\25-12-23\Master Data\04_FinanceSetup_DataTemplate.xlsx"
# r"C:\_IAL finance migration - Eteam\_Report\25-12-23\Master Data\06_CourseSetup_DataTemplate.xlsx"
# r"C:\_IAL finance migration - Eteam\_Report\25-12-23\Master Data\04_FinanceSetup_DataTemplate.xlsx"
compare_report_path: str = r"C:\_IAL finance migration - Eteam\_Report\25-12-23\ComparisonResult_20251223114555\Details\04_FinanceSetup_DataTemplate_report.xlsx"
# r"C:\_IAL finance migration - Eteam\_Report\25-12-23\ComparisonResult_20251223114555\Details\06_CourseSetup_DataTemplate_report.xlsx"
# r"C:\_IAL finance migration - Eteam\_Report\25-12-23\ComparisonResult_20251223114555\Details\04_FinanceSetup_DataTemplate_report.xlsx"

# Funding Agency
sheet_name: str = "SupplementaryFeeSetup"
# "CourseFee"
# "CourseFeeSetup"
# "FundingAgency"
# "SupplementaryFeeSetup"

# df_funding_agency_data_template: pd.DataFrame = pd.read_excel(data_template_path, sheet_name=sheet_name)
df_funding_agency_data_template: pd.DataFrame = pd.read_excel(
    data_template_path, sheet_name=sheet_name
)

# df_funding_agency_compare_report: pd.DataFrame = pd.read_excel(compare_report_path, sheet_name=sheet_name)
df_funding_agency_compare_report: pd.DataFrame = pd.read_excel(
    compare_report_path, sheet_name=sheet_name
)

# Reset column name
df_funding_agency_compare_report.columns = pd.Series(
    df_funding_agency_compare_report.columns.map(
        lambda x: np.nan if "Unnamed" in str(x) else x
    )
).ffill()

# Split Target vs Source
df_funding_agency_compare_report_source: pd.DataFrame = (
    df_funding_agency_compare_report.filter(like="Source")
)
df_funding_agency_compare_report_target: pd.DataFrame = (
    df_funding_agency_compare_report.filter(like="Target")
)

# Split Comparision result
df_compare_report_result: pd.DataFrame = df_funding_agency_compare_report.filter(
    like="Comparison Result"
)
df_compare_report_result.columns = pd.MultiIndex.from_frame(
    df_compare_report_result.loc[0, :].T.to_frame().reset_index()
)
df_compare_report_result = df_compare_report_result[1:]
df_compare_report_result.head()

# Reset column name
df_funding_agency_compare_report_source.columns = (
    df_funding_agency_compare_report_source.loc[0, :]
)
df_funding_agency_compare_report_source = df_funding_agency_compare_report_source[1:]

df_funding_agency_compare_report_target.columns = (
    df_funding_agency_compare_report_target.loc[0, :]
)
df_funding_agency_compare_report_target = df_funding_agency_compare_report_target[1:]

# Compare Tarfet vs Source
df_compare_result: pd.DataFrame = df_funding_agency_compare_report_source.compare(
    df_funding_agency_compare_report_target, result_names=("Source", "Target")
)
df_compare_result.head()

# Merge result with Comparision result
df_compare_result = df_compare_result.merge(
    df_compare_report_result, left_index=True, right_index=True, how="left"
)

# Write result to excel file
import datetime

result_date: str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
if not df_compare_result.empty:
    df_compare_result.index = df_compare_result.index + 2
    file_compare_result_path: str = f"{result_date}_compare_S_T_{sheet_name}.xlsx"
    df_compare_result.to_excel(
        file_compare_result_path, sheet_name=sheet_name, index=True
    )
    logger.info(f"Write to excel file: {file_compare_result_path}")
else:
    logger.info("No diff Source - Target")

# Compare origin vs Source
df_funding_agency_compare_report_source_compare = (
    df_funding_agency_compare_report_source.reset_index(drop=True).sort_values(
        by=df_funding_agency_compare_report_source.columns[:2].tolist(),
        ascending=True,
        ignore_index=True,
    )
)
if sheet_name == "FundingAgency":
    df_funding_agency_compare_report_source_compare = (
        df_funding_agency_compare_report_source_compare.drop("Category ID", axis=1)
    )

df_funding_agency_compare_report_source_compare.columns = (
    df_funding_agency_data_template.columns
)

df_compare_origin_vs_source_result = (
    df_funding_agency_data_template.sort_values(
        by=df_funding_agency_data_template.columns[:2].tolist(),
        ascending=True,
        ignore_index=True,
    )
    .astype(str)
    .compare(
        df_funding_agency_compare_report_source_compare.astype(str),
        result_names=("Origin", "Source"),
    )
)
if not df_compare_origin_vs_source_result.empty:
    df_compare_origin_vs_source_result.index = (
        df_compare_origin_vs_source_result.index + 2
    )
    file_compare_origin_vs_source_result_path: str = (
        f"{result_date}_compare_O_S_{sheet_name}.xlsx"
    )
    df_compare_origin_vs_source_result.to_excel(
        file_compare_origin_vs_source_result_path, sheet_name=sheet_name, index=True
    )
    logger.info(
        f"Write to excel file: {file_compare_origin_vs_source_result_path}.xlsx"
    )
else:
    logger.info("No diff Origin - Source")
