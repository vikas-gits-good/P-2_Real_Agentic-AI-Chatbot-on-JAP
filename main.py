import json
import asyncio
import pandas as pd
from src.ETL.ETL_config import MetadataConfig, CSJWebScrapeConfig
from crawl4ai import AsyncWebCrawler


async def extract(urls, config: CSJWebScrapeConfig):
    async with AsyncWebCrawler(config=config.browser_config) as crawler:
        results = await crawler.arun_many(
            urls=urls,
            # config=config.run_config_init_jsn,  # <- Working sort of
            config=config.run_config_init_bsf,  # <- Issues
            # config=config.run_config_tran,
            dispatcher=config.mem_ada_dispatcher,
        )
        for result in results:
            try:
                data = json.loads(result.extracted_content)
                print(data)
                pass

            except Exception as e:
                raise e


if __name__ == "__main__":
    mc = MetadataConfig(source="blog")
    df = mc.df_full["BlogCSJ"]
    urls = df["URL"].to_list()[:2]

    sc = CSJWebScrapeConfig(max_parallel=5, len_list=len(urls))
    asyncio.run(extract(urls=urls, config=sc))
