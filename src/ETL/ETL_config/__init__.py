import os
import pandas as pd
from typing import Literal
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
    KeywordRelevanceScorer,
)
from crawl4ai.deep_crawling.filters import (
    FilterChain,
    URLPatternFilter,
    DomainFilter,
    ContentTypeFilter,
    ContentRelevanceFilter,
)

from src.ETL.ETL_constants import RawData, BlogJSONSchema


class MetadataConfig:
    def __init__(self, source: Literal["video", "blog"] = "video"):
        if source == "video":
            df_csj, df_rp = [
                pd.read_excel(
                    io=RawData.EXCEL_PATH,
                    sheet_name=sheet,
                )
                for sheet in [RawData.SHEET_NAME_CSJ_VIDS, RawData.SHEET_NAME_RP_VIDS]
            ]
            df_csj, df_rp = [df.dropna(axis=0, inplace=False) for df in [df_csj, df_rp]]

            self.df_full = pd.concat([df_csj, df_rp], axis=0)

        elif source == "blog":
            df_csj, df_rp = [
                pd.read_excel(
                    io=RawData.EXCEL_PATH,
                    sheet_name=sheet,
                )
                for sheet in [RawData.SHEET_NAME_CSJ_BLOG, RawData.SHEET_NAME_RP_BLOG]
            ]
            df_csj, df_rp = [df.dropna(axis=0, inplace=False) for df in [df_csj, df_rp]]
            self.df_full = {"BlogCSJ": df_csj, "BlogRP": df_rp}


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
    def __init__(self, max_parallel: int = 5, len_list: int = 0) -> None:
        self.browser_config = BrowserConfig(
            browser_type="chromium",
            headless=True,
            verbose=False,
        )
        json_extract_strat_init = JsonCssExtractionStrategy(
            BlogJSONSchema.SCHEMA_CSJ_BLOG_INIT, verbose=False
        )
        json_extract_strat_tran = JsonCssExtractionStrategy(
            BlogJSONSchema.SCHEMA_CSJ_BLOG_PAGE, verbose=False
        )

        scorer = KeywordRelevanceScorer(
            keywords=[BlogJSONSchema.PAGINATION_WORD], weight=1.2
        )

        # Configure the strategy
        filter_chain = FilterChain(
            [
                ContentRelevanceFilter(
                    query=BlogJSONSchema.PAGINATION_WORD, threshold=0.95
                ),
            ]
        )
        bfc_crawl_strat = BestFirstCrawlingStrategy(
            max_depth=5,
            include_external=False,
            url_scorer=scorer,
            max_pages=7,
            filter_chain=filter_chain,
        )

        self.run_config_init_bsf = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=json_extract_strat_init,
            deep_crawl_strategy=bfc_crawl_strat,
            js_code=[BlogJSONSchema.JS_WAIT_TIME],
            exclude_external_links=True,
        )

        self.run_config_init_jsn = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=json_extract_strat_init,
            js_code=[BlogJSONSchema.JS_WAIT_TIME],
            # stream=True,
        )
        self.run_config_tran = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=json_extract_strat_tran,
            js_code=[BlogJSONSchema.JS_WAIT_TIME],
            # stream=True,
        )
        rate_limiter = RateLimiter(
            base_delay=(2, 8),
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
