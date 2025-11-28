import os
import pandas as pd
from typing import Literal
from dotenv import load_dotenv
from youtube_transcript_api.proxies import WebshareProxyConfig, GenericProxyConfig

from src.ETL.ETL_constants import RawData


class MetadataConfig:
    def __init__(self):
        self.df_csj = pd.read_excel(
            io=RawData.EXCEL_PATH,
            sheet_name=RawData.SHEET_NAME_CSJ,
        )
        self.df_csj.dropna(axis=0, inplace=True)
        self.df_rp = pd.read_excel(
            io=RawData.EXCEL_PATH,
            sheet_name=RawData.SHEET_NAME_RP,
        )
        self.df_rp.dropna(axis=0, inplace=True)

        self.df_full = pd.concat([self.df_csj, self.df_rp], axis=0)
        self.raw_csj_free_path = RawData.RAW_CSJ_FREE
        self.raw_rp_free_path = RawData.RAW_RP_FREE


class ProxyConfig:
    def __init__(
        self,
        provider: Literal["WebShare", "Other"] = "WebShare",
    ) -> None:
        load_dotenv("src/Secrets/Secrets.env")

        if provider == "WebShare":
            self.proxy_config = WebshareProxyConfig(
                proxy_username=os.getenv("WEBSHARE_USERNAME", ""),
                proxy_password=os.getenv("WEBSHARE_PASSWORD", ""),
                filter_ip_locations=["in"],
                domain_name="p.webshare.io",
                proxy_port=80,
            )

        elif provider == "Other":
            self.proxy_config = GenericProxyConfig(
                https_url="",
                # http_url="",
            )
