import pandas as pd
from dataclasses import dataclass


@dataclass
class RawData:
    EXCEL_PATH = "src/Data/Raw_Data_Links.xlsx"
    SHEET_NAME_CSJ = "CSJ"
    SHEET_NAME_RP = "RP"
    RAW_CSJ_FREE = "src/Data/1_Raw/CSJ/Free"
    RAW_CSJ_PAID = "src/Data/1_Raw/CSJ/Paid"
    RAW_RP_FREE = "src/Data/1_Raw/RP/Free"
    RAW_RP_PAID = "src/Data/1_Raw/RP/Paid"
