# %%
# # %%
# user_info_compare_file_path: str = r"C:\Users\rian.pham\Downloads\ComparisionResult_batch1\ComparisionResult_batch1\Detail\Data Template M01_User Information_C2R1_23_Jan_2026_LXP_Original report.xlsx"
# master_data_setup_compare_file_path: str = r"C:\Users\rian.pham\Downloads\ComparisionResult_batch1\ComparisionResult_batch1\Detail\Data Template M02_Master Data Setup_C2R1_23Jan2026_CMS_Original report.xlsx"
# finance_process_compare_report_group_1_file_path: str = (
#     r"C:\Users\rian.pham\Downloads\Compare_Finance Process_Original report.xlsx"
# )
# sheet_name_list: list[str] = [
#     "BillingAdvice",
#     "BillingAdviceInstalment",
#     "BillingAdviceBillingCharges",
#     "BillingAdviceSubsidy",
#     "BillingAdviceMiscOnly",
#     "BillingAdvicePayment",
#     "BillingAdviceInvoice",
# ]
# for sheet_name in sheet_name_list:
#     df = pl.read_excel(
#         finance_process_compare_report_group_1_file_path, sheet_name=sheet_name
#     ).to_pandas()
#     df.columns = pd.Series(
#         df.columns.map(lambda x: np.nan if "UNNAMED" in str(x) else x)
#     ).ffill()
#     df_source: pd.DataFrame = df.filter(like="Source")
#     df_source.columns = df_source.loc[0, :]
#     df_source = df_source[1:]
#     df_target: pd.DataFrame = df.filter(like="Destination")
#     df_target.columns = df_target.loc[0, :]
#     df_target = df_target[1:]
#     df_compare_report_result: pd.DataFrame = df.filter(like="Comparison Result")
#     df_compare_report_result.columns = pd.MultiIndex.from_frame(
#         df_compare_report_result.loc[0, :].T.to_frame().reset_index()
#     )
#     df_compare_report_result = df_compare_report_result[1:]
#     df_trasformation = df.filter(like="Transform Details")
#     df_trasformation.columns = pd.MultiIndex.from_frame(
#         df_trasformation.loc[0, :].T.to_frame().reset_index()
#     )
#     df_trasformation = df_trasformation[1:]
#     compare_res = df_source.compare(
#         df_target,
#         result_names=("Source", "Destination"),
#         keep_equal=False,
#         keep_shape=False,
#     )
#     index_level_0 = compare_res.columns.get_level_values(0).unique()
#     index_level_1 = compare_res.columns.get_level_values(1).unique()
#     # logger.info(index_level_0)
#     # logger.info(index_level_1)
#     compare_res = compare_res.merge(
#         df_compare_report_result, left_index=True, right_index=True, how="left"
#     )
#     compare_res = compare_res.merge(
#         df_trasformation, left_index=True, right_index=True, how="left"
#     )
#     compare_res = compare_res.loc[
#         compare_res[("Comparison Result", "Comparison Status")].isin(
#             ["Object Mismatch", "Field Mismatch"]
#         )
#     ]
#     if not compare_res.empty:
#         cols_to_drop = []
#         for col in index_level_0:
#             pair = [
#                 (col, index_level_1[0]),
#                 (col, index_level_1[1]),
#             ]
#             if compare_res[pair[0]].isna().all() and compare_res[pair[1]].isna().all():
#                 cols_to_drop.extend(pair)
#         compare_res = compare_res.drop(columns=cols_to_drop)
#         compare_res = compare_res.swaplevel(0, 1, axis=1)
#         compare_res = compare_res.reset_index()
#         compare_res["index"] = compare_res["index"] + 1
#         header1 = pd.DataFrame(
#             [compare_res.columns.get_level_values(0)],
#         )
#         header2 = pd.DataFrame(
#             [compare_res.columns.get_level_values(1)],
#         )
#         body = compare_res.copy()
#         body.columns = range(len(body.columns))
#         result = (
#             pd.concat(
#                 [header1, header2, body],
#                 ignore_index=True,
#             )
#             .fillna("")
#             .astype(str)
#         )
#         pl.from_pandas(result).write_parquet(f"{sheet_name}_3.parquet")
#         # pass
#     else:
#         logger.info("No data to write.")
#     logger.info(f"Complete sheet: {sheet_name}")
# logger.info(
#     f"Complete all sheet in file {finance_process_compare_report_group_1_file_path}"
# )
# %%
import datetime
import os
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd
from jinja2.utils import concat
from loguru import logger

import polars as pl

today = datetime.datetime.now().strftime(format="%Y-%m-%d")

output_folder_path: str = f"./output_compare/{today}"
if not os.path.exists(output_folder_path):
    os.makedirs(output_folder_path)

# finance_process_compare_report_group_1_file_path: str = r"C:\Users\rian.pham\Downloads\Data Template T07_Finance Process_C2R2_02_Jul_2026_Original report (2).xlsx"
# (
#     r"C:\Users\rian.pham\Downloads\Compare_Finance Process_Original report.xlsx"
# )
# sheet_name_list: list[str] = ["HonorariumRecord", "HonorariumBatch"]
# [
#     "BillingAdvice",
#     "BillingAdviceInstalment",
#     "BillingAdviceBillingCharges",
#     "BillingAdviceSubsidy",
#     "BillingAdviceMiscOnly",
#     "BillingAdvicePayment",
#     "BillingAdviceInvoice",
# ]
from pathlib import Path


def get_sheet_names(file_path: Path) -> list[str]:
    """Read sheet names without loading any data."""
    from python_calamine import CalamineWorkbook

    wb = CalamineWorkbook.from_path(str(file_path))
    sheet_names = [sheet for sheet in wb.sheet_names if sheet != "Config Data"]
    logger.info(sheet_names)
    return sheet_names


def process_sheet(file_path, sheet_name):
    # df_all = pl.read_excel(
    #     finance_process_compare_report_group_1_file_path, sheet_name=sheet_name_list
    # )

    logger.info(f"Processing sheet: {sheet_name}")
    df = pl.read_excel(file_path, sheet_name=sheet_name).to_pandas()

    df.columns = pd.Series(
        df.columns.map(lambda x: np.nan if "UNNAMED" in str(x) else x)
    ).ffill()

    df_source: pd.DataFrame = df.filter(like="Source")
    df_source.columns = df_source.loc[0, :]
    df_source = df_source[1:]

    df_target: pd.DataFrame = df.filter(like="Destination")
    df_target.columns = df_target.loc[0, :]
    df_target = df_target[1:]

    df_compare_report_result: pd.DataFrame = df.filter(like="Comparison Result")
    df_compare_report_result.columns = pd.MultiIndex.from_frame(
        df_compare_report_result.loc[0, :].T.to_frame().reset_index()
    )
    df_compare_report_result = df_compare_report_result[1:]

    df_trasformation = df.filter(like="Transform Details")
    df_trasformation.columns = pd.MultiIndex.from_frame(
        df_trasformation.loc[0, :].T.to_frame().reset_index()
    )
    df_trasformation = df_trasformation[1:]

    compare_res = df_source.compare(
        df_target,
        result_names=("Source", "Destination"),
        keep_equal=False,
        keep_shape=False,
    )

    index_level_0 = compare_res.columns.get_level_values(0).unique()
    index_level_1 = compare_res.columns.get_level_values(1).unique()
    # logger.info(index_level_0)
    # logger.info(index_level_1)

    compare_res = compare_res.merge(
        df_compare_report_result, left_index=True, right_index=True, how="left"
    )
    compare_res = compare_res.merge(
        df_trasformation, left_index=True, right_index=True, how="left"
    )

    compare_res = compare_res.loc[
        compare_res[("Comparison Result", "Comparison Status")].isin(
            ["Object Mismatch", "Field Mismatch"]
        )
    ]

    if not compare_res.empty:
        cols_to_drop = []
        for col in index_level_0:
            pair = [
                (col, index_level_1[0]),
                (col, index_level_1[1]),
            ]

            if compare_res[pair[0]].isna().all() and compare_res[pair[1]].isna().all():
                cols_to_drop.extend(pair)

        compare_res = compare_res.drop(columns=cols_to_drop)

        # compare_res = compare_res.swaplevel(0, 1, axis=1)
        compare_res = compare_res.reset_index()
        compare_res["index"] = compare_res["index"] + 1

        header1 = pd.DataFrame(
            [compare_res.columns.get_level_values(0)],
        )

        header2 = pd.DataFrame(
            [compare_res.columns.get_level_values(1)],
        )

        body = compare_res.copy()
        body.columns = range(len(body.columns))

        result = (
            pd.concat(
                [header1, header2, body],
                ignore_index=True,
            )
            .fillna("")
            .astype(str)
        )
        # result.columns = result.loc[0, :]
        # result = result.loc[1:, :]

        pl.from_pandas(result).write_csv(
            f"{output_folder_path}/{sheet_name}.csv", include_header=False
        )

        # pass
    else:
        logger.info(f"No data to write for {sheet_name}.")
    logger.info(f"Complete sheet: {sheet_name}")


if __name__ == "__main__":
    import glob

    folder_path: str = r"C:\Users\rian.pham\Downloads\ComparisionResult_20260601152733\ComparisionResult_20260601152733\Detail"
    folder = Path(folder_path)
    files = list(folder.glob("*.xlsx"))
    for file in files:
        sheet_name_list = get_sheet_names(file)
        with ProcessPoolExecutor(max_workers=max(4, len(sheet_name_list))) as pool:
            list(
                pool.map(process_sheet, [file] * len(sheet_name_list), sheet_name_list)
            )
        logger.info(f"Complete all sheet in file {file}")
    # with ProcessPoolExecutor(max_workers=max(4, len(sheet_name_list))) as pool:
    #     list(pool.map(process_sheet, sheet_name_list))
    # logger.info(
    #     f"Complete all sheet in file {finance_process_compare_report_group_1_file_path}"
    # )

# %%
# headers: dict = {
#     "accept": "application/json",
#     "accept-language": "en-US,en;q=0.9",
#     "content-type": "application/json",
#     "downlink": "10",
#     "priority": "u=1, i",
#     "sec-ch-ua": "\"Not:A-Brand\";v=\"99\", \"Microsoft Edge\";v=\"145\", \"Chromium\";v=\"145\"",
#     "sec-ch-ua-mobile": "?0",
#     "sec-ch-ua-platform": "\"Windows\"",
#     "sec-fetch-dest": "empty",
#     "sec-fetch-mode": "cors",
#     "sec-fetch-site": "same-origin",
#     "x-language": "en-US",
#     "x-requested-with": "XMLHttpRequest",
#     "x-timezone-id": "Singapore Standard Time",
#     "x-timezone-offset": "-480",
#     "cookie": "_cfuvid=bz1soPT_85WJsN1x2TWUg7iFyG3LnqGLwEXTAVnaCT8-1773037245.0187306-1.0.1.1-2RNCtZV3yK.kzUwQ1L1s4zHLvcIWmA.3SBCdKFrxhx8; vitae=3ZHAYD8TY3PzEhSmfyHABcQhe634YJLVda1Yvd2E%2B5hXtETwed06Q%2FkBqVTJ%2Fncj8FTOOKrSrllLOCJDpWCpdZFM%2BQFhgcVWJUCLBajZiCiUkqjEOlC9pIpAXcQCVcbCJHnBdO3rXowNAd3RLEstC0yFaB37d5yxkRMnb4guMTgNdnL1Ao1E8elzCcfIHk5z; maiven=_f8Isy2fTsU-WzQjQw7sbD-LhCt3Op7BC9rtI3KKwJLCmYD1xSJ_UPn9bRk-timvr0tJbCYU7a35VltwGHBBuIcHAGY7CMFWYp5in1vLNxA-UjzyzMz5sYX0Y6O8BaUqlFwBnRU_8wiULeXRgplDYVeXgpPkxzohiWBbLhuYx-aHILJZvpGykI_XSpuAyWaCaSHyy4wLQEhMSFRA_jQFTlbSQxDZoTlJz2ToVG-OP9lHiQyygyPapJW4a2ze4LAK9wE-dwx-rvHG-AOaAwHebgVk1Xr_HkDUKXdyBNTCeBskhjwP__Hy7zdWIfqVGAo_9lWiH7NIkXR4we-0v9TKtcjs1jBKmxG0F2rwgUb5K4ORBVK8tYsBY34AHB9JMYhBXTNXaXNcVxr276SOHY9odbuDSI_-YRIGj6xDtyMK0s1jkrj6qgMBK20KTUkhQKorq9gnnzALAbE0Fi2ge-NUPW69Hh46aQ9EjEGx6uHK9RuvOg9vyD6gpjE-x3CvUwDCIIiaJbc9r8aHSLiqxq4uHzlxRSugr3bzbNdpvdR-c9HI-jVFGW6pepcHAUDsFoC8laFuW2X8O_1efG-TxVO1pAMiutfIWf_lWtZma-VF4QwurdEmXUVG45DSmJAcOrLIUl3CpAcl4Nqer3u3_9hbeqgsRlsiEZKdQpg0R0eupvrztmomrMcKtD1ndy4ZxGYCg_w7NRCSlN1Xg7sukKnWqI7kaDV6Fa4H8YK095DZhhO5BoAJ9Rs8x6NLRskKCbHXXGXl9ah9UM2V7GXGSSvGIhnmghpimz6CHpac3xGEocXCwoS9A45PxmFCARTe3AuKT8GQT4bY2TVuuWKuS_Cby9GGa5qXXFdyte7PsQRuYgZZcr-TJjBhZaxtJu5Ol5gMwpIHpf-p1MczX6_oBjZU4KXzQu6j5WuJmX1aLakYY1-TbT5yCQZk6a04R1LXcwmKbnBj7T5uQVmGXkWYhfYLHToiT_xYCYaApfJRI0p9sGShA3l1gT4_fFuUdefKMlN-BSEnXoxm2894fMxfzoOEylRw1ibJmNKgJqP4m5XIKzcSEF8iohaq6i4QKEzoaR1PoF5IocpQpxQ7yuMopxaTTsyowSvVUSc7CtVoeruGIePeRU-nM76Fv8bxd4Ya615lBth5JWPSCP8XMLu6dnwDspEaft3kTg4tLvWOORNx2w7JMyldST9C0DuTfKDZ_GN0rMu4sJCEyYCmAAa9-t4Zkqwi6Gbqj_FJw7mSmeVqxoGwaqB-rgM6vwAX_HcUeEWTafwU3rU57w2pTUibP7_jjwhoE8IToh0jyH6kjgl-iFPqgahTGD90J6rkJcApCs-x_gujRJIQcHTzSqpbyPL5f_ggr-tRBmgkronRmggde0w9tutdA3YQCIzCwHJ759CYDql4FHmMgHb3WoJ0-lKNkaat0IK5HV3W0MtB-w--5yw_bO6PZHdFRzGZYZ6qEQmt6VpawBzPsfO41nsZabmgNuv4iZOmMAgCaGrpwDGdBke9wWaDdwzbYv-YxCMd957rl4PfWG3KXf0F-789c1gY8VgmzNeb3xBm1NL7ZHTapA7Qsh7bdEo0woLGY4f05mJtree4wAeN512MVgRSBrwlUPkXo_1pk-wUZ7ArrZwJcx6qMvzw9azkp13Jz9_H_jqSdE0h7f_n_jd51Eete_MLGjOOG9tyJarwvuieu8ih1ZlIOn-oMAHHQQ9W3PzRlQOjMaUGQ-JAJ8T48kgVi45ceb7CXWXTHn8s0oz9BkbzdMmDAnvEH_qxHu9GDJC18AaufH_wTc0AzLLzNcZWx-kVvvhmgXUWP7uFVoRi9QFW2UHbRpI8GWH5IcAR0JIgUMBwiDG80SaNcxoz7dRTYxjjp1HwZ_SjTgUnq3MagoqHfG2Ub0ULnCJm90RJvpFrX5GCdt6EanW4lnGUXE0iak7lGpVKQDVoJIt14LZLkEo24BsqhN6lEIFlxYt06b3626uQqQsqarI_GcCWLWF1-fgpZVncZ1zRXNJVKySNtDPsmWTuBjIZy7pC3pHMV_0-iPCXS8uUZatKQYR0DYergZZbYpTxlpIUAsE6tldpBmbK3LfptXtkDwSE2dm7xvUvyf9X5ozKC8NeWbvOdRCgGD_jsZTCmSfAbPuQrXiEY5UDT_teid00MQ9gorv6PdXw9-laQ0XxJGyXb0COm1ZdqavGk60-MGwCXBLjldrMc9tWfTk2saSa80y3Shy97cEue7eQcRwS7IGgJHwKoqRA8auBcmavBC0ev-hJ3FYdbo7zntlbHxuLN3wWyadaN2WmWEONdXkL-Pn6PmglGpabMNQi90CD-eRwyPMhtN5EMD1X3lYlb_yV7IF-GFHrg1d2eghQy-IM0uXPNSQkR6oct3rN9lH_4f55k7Z3CUzvM_jJQsEcsyYeQfqDWs4CJHvDOU5nI1XqdeFwnbTKfRwbSz-RD9U3mptXfD5b_zywR0pUK8vfWEArKnZow45fz-KsR3sFUTHKVYBfWIT4R95VimQFfeusGgk-tb3lof6xBALmpLgN9eidLIzy5OAykW1RwASpjZzRxiALJSTjVVB0jf4LezildpX6AlqOcYxcUQIOv5xOrD3GK4TOSnjjAnTAtfpPdZRWgHr7HJ6A-5QJgm3fobQV-49AGoA_wxsPs666FD2FoCXHi_mJ0xYJr2oEJLB-z9LRohJmr7GZb1FPbJqkjs2HGO5egC2pFKjMdgst_LV_myyDCrTImziON4yOH4BvIE06zDBRkqbyzLuFCVQz1-0Xp01YEoHEX_tFBEhR0RsvsauVhu_mbmKrZagFWtTRVp26lVvHhHeCQEWALr7vaJ8e0T4T0-wwZgxLtcCqJiEmqqH-dpUoQQNaMyHInirJnqx6-X8VEwI7Qk58JjeGtA3OX1Xs1ID6iVXoVzyf5av5D4L6Y9U4te6mfMIz8DBmDtwWjS4_E92Bv8tJJEfdTjgobUJ1zTDoQunYy_7ldVRIaGxPrSuiSwUetwubTOIHpULsWMUULrTb4CKzuoNB7DSZB0ZNaHGT2HKm8l1eCaQ; _cfuvid=PW.PHU.kwbXEuK418llSOZ6Fss.DqANeZxztqgyPyOc-1773041132089-0.0.1.1-604800000; cf_clearance=Xplb2JxGtTMX8ZLX1lxTbkMTEBfrYhVlwCvlKMDlTbs-1773042513-1.2.1.1-xEovdS9gmYg8FiSrk1iaBLtkgZp9vieaOrl2jjbsDvx9_XoaaZbcdRXrbpFCzfepRJQ3KScs4YXkVoPdxsJtwFNI0eQXGCGXA5Zt9ejxM9jKfx_u6ZM5dLkYsa_18HRvK5yHGYOJLwaDRn27pIDzV4TNR6OIDeb5zQ6yf7HGowPm4CRjD3siAGuCLI_fuIVvzQ8kjroBF71T1Se0Uj5qG1tE5oACgw5YD7WqS92UqQ4; __cf_bm=UJzfI6Qb2yS.cD7XHB_CgPisr6lEf9CPERfEzWYJ.vk-1773042513.9787052-1.0.1.1-KPSzY1ZqTec_V2ZqOidfP25wf69KQCzFrfH204K..dYxzAZ7Htzi2LlngAa1U_msAnkFmv_u_SN.cd21KSiNRUaH1q2ySlNg6Vse5XbAtsP5aN0saYsaIkSPjMRKy_ZO",
#     "Referer": "https://awb.ntuclearninghub.com/tms/admin/configuration/intake/view/tag/3afb5b14-38b8-4d82-9660-f791d0b2fb0c/f3574c7c-3761-5ba6-286a-078ae7bc6cdb/1"
#   }

# # from requests import get, post
# # import httpx
# from curl_cffi import requests

# # get all intake id from CourseIntakeMiscellDefaultFee
# import pandas as pd
# import polars as pl
# intake_file_path: str = r"C:\Users\rian.pham\Downloads\Data Template M10_Course IntakeSetupAndOthers_C2R1_24_Feb_2026.xlsx"
# df_intake_misc_default_fee = pl.read_excel(intake_file_path, sheet_name="CourseIntakeMiscellDefaultFee").to_pandas()
# df_intake_misc_default_fee = df_intake_misc_default_fee["CourseIntakeId"].unique().tolist()
# lack_of_misc_item_list: list = []

# search_intake_api: str = "https://awb.ntuclearninghub.com/tms/admin/api/v1/courseintake/paging"
# search_intake_request_body: dict = {
#     "pageIndex": 1,
#     "pageSize": 10_000,"isAscending":False,"searchContent":"","filters":{"BlendedCourse":[],"CourseCategory":[],"CourseType":[],"ModeOfTraining":[],"Venue":[],"OpenTo":[],"UEN":[],"Branch":[],"School":[],"StartDate":[],"Status":[]}}

# get_financial_setup_api: str = "https://awb.ntuclearninghub.com/tms/admin/api/v1/courseintakefee/financialSetup/getexist?courseintakeid={}&courseversion=1"


# for item in df_intake_misc_default_fee:
#     print(f"Intake: {item}")
#     search_intake_request_body["searchContent"] = item

#     search_res = requests.post(search_intake_api, headers=headers, json=search_intake_request_body, impersonate="chrome",)
#     if search_res.status_code != 200:
#         print(f"Error: {search_res.status_code}, {search_res.text}")
#         break
#     search_res_json = search_res.json()
#     if search_res.get("code") != 200 or search_res_json.get("status") != 0:
#         print(f"Error: {search_res_json}")
#         break
#     intake = search_res_json.get("data", {}).get("items", [])
#     if not intake:
#         print(f"Error: No intake found")
#         break
#     intake_id = intake[0].get("id", "")
#     financial_setup_api = get_financial_setup_api.format(intake_id)
#     financial_setup_res = requests.get(financial_setup_api, headers=headers)
#     if financial_setup_res.status_code != 200:
#         print(f"Error: {financial_setup_res.status_code}, {financial_setup_res.text}")
#         break
#     financial_setup_res_json = financial_setup_res.json()
#     if financial_setup_res.get("code") != 200 or financial_setup_res_json.get("status") != 0:
#         print(f"Error: {financial_setup_res_json}")
#         break
#     financial_setup_res_json = financial_setup_res_json.get("data", {}).get("miscellaneous", [])
#     if not financial_setup_res_json:
#         print(f"Error: No financial setup found")
#         lack_of_misc_item_list.append(item)
#     misc_item = [item for item in financial_setup_res_json if item.get("type") == 5]
#     if len(financial_setup_res_json) == 0:
#         print(f"Error: Not misc item found")
#         lack_of_misc_item_list.append(item)
# print(lack_of_misc_item_list, len(lack_of_misc_item_list))
