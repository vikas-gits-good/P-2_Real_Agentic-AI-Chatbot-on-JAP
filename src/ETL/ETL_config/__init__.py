import os
import json
import asyncio
import pandas as pd
from typing import Literal, List, Dict
from dotenv import load_dotenv
from youtube_transcript_api.proxies import WebshareProxyConfig, GenericProxyConfig
from crawl4ai import (
    CacheMode,
    RateLimiter,
    CrawlerMonitor,
    BrowserConfig,
    CrawlerRunConfig,
    MemoryAdaptiveDispatcher,
    JsonCssExtractionStrategy,
    BestFirstCrawlingStrategy,
    URLPatternFilter,
    FilterChain,
)

from src.ETL.ETL_constants import RawData, BlogJSONSchema


class MetadataConfig:
    def __init__(self, source: Literal["video", "blog"] = "video"):
        def _get_dataframe(sheet_list: list[str]) -> pd.DataFrame:
            df_csj, df_rp = [
                pd.read_excel(
                    io=RawData.EXCEL_PATH,
                    sheet_name=sheet,
                )
                for sheet in sheet_list
            ]
            df_csj, df_rp = [
                df.dropna(axis=0, inplace=False, ignore_index=True)
                for df in [df_csj, df_rp]
            ]
            return pd.concat([df_csj, df_rp], axis=0)

        if source == "video":
            self.df_full = _get_dataframe(
                sheet_list=[RawData.SHEET_NAME_CSJ_VIDS, RawData.SHEET_NAME_RP_VIDS]
            )
        elif source == "blog":
            self.df_full = _get_dataframe(
                sheet_list=[RawData.SHEET_NAME_CSJ_BLOG, RawData.SHEET_NAME_RP_BLOG]
            )


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


class CSJWebScrapeConfig:
    def __init__(self, max_parallel: int = 2, len_list: int = 0) -> None:
        self.browser_config = BrowserConfig(
            browser_type="chromium",
            headless=True,  # False,  #
            verbose=False,
        )
        # strat to get initial links
        json_extract_strat_init = JsonCssExtractionStrategy(
            schema=BlogJSONSchema.SCHEMA_CSJ_BLOG_INIT, verbose=False
        )
        # strat to get transcripts
        json_extract_strat_tran = JsonCssExtractionStrategy(
            schema=BlogJSONSchema.SCHEMA_CSJ_BLOG_PAGE, verbose=False
        )

        # filter to deepcrawl through pagination
        filter_chain = FilterChain([URLPatternFilter(patterns=["*/page/*"])])
        # deep crawl strat
        bsfc_crawl_strat = BestFirstCrawlingStrategy(
            max_depth=10,
            filter_chain=filter_chain,
        )
        # run config for init with deepcrawl
        self.run_config_init_bsf = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=json_extract_strat_init,
            deep_crawl_strategy=bsfc_crawl_strat,
            js_code=[BlogJSONSchema.JS_WAIT_TIME],
            exclude_external_links=True,
        )
        # run config for transcript
        self.run_config_tran = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=json_extract_strat_tran,
            js_code=[BlogJSONSchema.JS_WAIT_TIME],
        )
        rate_limiter = RateLimiter(
            base_delay=(2, 4),
            max_delay=20.0,
            max_retries=3,
            rate_limit_codes=[429, 503],
        )
        crawl_monitor = CrawlerMonitor(
            urls_total=len_list,
            refresh_rate=0.1,
            enable_ui=True,
            max_width=120,
        )
        self.mem_ada_dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=80.0,
            max_session_permit=max_parallel,
            check_interval=1.0,
            rate_limiter=rate_limiter,
            monitor=crawl_monitor,
        )


class RPWebScrapeConfig:  # This is placeholder. Needs to be updated later
    def __init__(self, max_parallel: int = 2, len_list: int = 0) -> None:
        self.browser_config = BrowserConfig(
            browser_type="chromium",
            headless=True,  # False,  #
            verbose=False,
        )
        # strat to get initial links
        json_extract_strat_init = JsonCssExtractionStrategy(
            schema=BlogJSONSchema.SCHEMA_CSJ_BLOG_INIT, verbose=False
        )
        # strat to get transcripts
        json_extract_strat_tran = JsonCssExtractionStrategy(
            schema=BlogJSONSchema.SCHEMA_CSJ_BLOG_PAGE, verbose=False
        )

        # filter to deepcrawl through pagination
        filter_chain = FilterChain([URLPatternFilter(patterns=["*/page/*"])])
        # deep crawl strat
        bsfc_crawl_strat = BestFirstCrawlingStrategy(
            max_depth=10,
            filter_chain=filter_chain,
        )
        # run config for init with deepcrawl
        self.run_config_init_bsf = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=json_extract_strat_init,
            deep_crawl_strategy=bsfc_crawl_strat,
            js_code=[BlogJSONSchema.JS_WAIT_TIME],
            exclude_external_links=True,
        )
        # run config for init with basic setting
        self.run_config_init_jsn = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=json_extract_strat_init,
            js_code=[BlogJSONSchema.JS_WAIT_TIME],
        )
        # run config for transcript
        self.run_config_tran = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=json_extract_strat_tran,
            js_code=[BlogJSONSchema.JS_WAIT_TIME],
        )
        rate_limiter = RateLimiter(
            base_delay=(2, 4),
            max_delay=20.0,
            max_retries=3,
            rate_limit_codes=[429, 503],
        )
        crawl_monitor = CrawlerMonitor(
            urls_total=len_list,
            refresh_rate=0.1,
            enable_ui=True,
            max_width=120,
        )
        self.mem_ada_dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=80.0,
            max_session_permit=max_parallel,
            check_interval=1.0,
            rate_limiter=rate_limiter,
            monitor=crawl_monitor,
        )
