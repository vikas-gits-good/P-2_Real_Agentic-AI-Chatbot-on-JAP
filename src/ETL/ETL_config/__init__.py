import os
import pandas as pd
from typing import Literal
from dotenv import load_dotenv
from youtube_transcript_api.proxies import WebshareProxyConfig, GenericProxyConfig

from src.ETL.ETL_constants import RawData


class MetadataConfig:
    def __init__(self, source: Literal["CSJ", "RP"] = "CSJ"):
        sheet = RawData.SHEET_NAME_CSJ if source == "CSJ" else RawData.SHEET_NAME_RP
        self.df = pd.read_excel(io=RawData.EXCEL_PATH, sheet_name=sheet)


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
            ot_username = os.getenv("OTHER_USERNAME", "")
            ot_password = os.getenv("OTHER_PASSWORD", "")
            self.proxy_config = GenericProxyConfig(
                https_url="",
                # http_url="",
            )
