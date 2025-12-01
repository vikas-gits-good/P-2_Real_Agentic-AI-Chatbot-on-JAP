import os
import json
import textwrap
from typing import Literal, List
from crawl4ai import AsyncWebCrawler
from crawl4ai.models import CrawlResultContainer
from pytube import Playlist, YouTube
from youtube_transcript_api import YouTubeTranscriptApi
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.ETL.ETL_utils import check_duplicates
from src.ETL.ETL_constants import RawData
from src.ETL.ETL_config import (
    MetadataConfig,
    ProxyConfig,
    CSJWebScrapeConfig,
    RPWebScrapeConfig,
)

from src.Logging.logger import log_etl
from src.Exception.exception import CustomException, LogException


class YouTubeTranscriptWriter:
    """Extract `free video content` transcript from youtube."""

    def __init__(
        self,
        metadata: MetadataConfig = MetadataConfig(source="video"),
        proxy_rotation_config: ProxyConfig = ProxyConfig(provider="WebShare"),
    ) -> None:
        self.proxy_config = proxy_rotation_config.proxy_config
        self.df_full = metadata.df_full
        self.full_files = check_duplicates(data=self.df_full)

    def _process_video(self, i, j, video_url, save_folder):
        try:
            # get video details
            yt = YouTube(video_url)
            file_name = self.full_files["vid_name"][i][j]
            log_etl.info(f"Extract: Processing {file_name}")

            # get video transcript
            yt_ts_api = YouTubeTranscriptApi(proxy_config=self.proxy_config)
            transcript_list = yt_ts_api.list(yt.video_id)
            transcript = transcript_list.find_transcript(["en"])
            video_transcript = transcript.fetch()
            transcript_text = " ".join([snippet.text for snippet in video_transcript])
            transcript_text = "\n".join(textwrap.wrap(transcript_text, width=160))

            # make save folder
            if not os.path.exists(save_folder):
                os.makedirs(save_folder, exist_ok=True)

            # write data
            file_path = os.path.join(save_folder, file_name)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(f"{file_name[:-4]}\n\n")
                f.write(f"{video_url}\n\n")
                f.write(transcript_text)

            log_etl.info(f"Extract: Saving {file_name}")

        except Exception as e:
            LogException(e, "Extract", log_etl)
            raise CustomException(e)

    def run(self):
        try:
            log_etl.info(
                f"Extract: Processing {len(self.full_files['pl_url'])} seasons in total"
            )
            for i in range(len(self.full_files["pl_url"])):
                pl = Playlist(self.full_files["pl_url"][i])
                vl = pl.video_urls

                log_etl.info(
                    f"Extract: Parallel processing '{self.full_files['sv_path'][i]}'"
                )
                save_dir = f"{RawData.RAW_CSJ_FREE}/{self.full_files['sv_path'][i]}/"

                # Process videos in parallel with max 10 threads
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = []
                    for j, video_url in enumerate(vl):
                        futures.append(
                            executor.submit(
                                self._process_video, i, j, video_url, save_dir
                            )
                        )

                    # Wait for all futures to complete
                    for future in as_completed(futures):
                        pass

        except Exception as e:
            LogException(e, "Extract", log_etl)
            raise CustomException(e)


class BlogTranscriptWriter:
    """Extract `free blog content` transcript from Blog."""

    def __init__(
        self,
        # content: Literal["Video", "Article"] = "Video",
        method: Literal["series", "parallel"] = "series",
    ) -> None:
        # self.content = content
        self.method = method
        self.data = MetadataConfig(source="blog").df_full
        self.urls = self.data["URL"].to_list()
        self.crw_csj_config = CSJWebScrapeConfig(
            max_parallel=5,
            len_list=len(self.urls),
        )
        self.crw_rp_config = RPWebScrapeConfig(
            max_parallel=5,
            len_list=len(self.urls),
        )

    async def _process_blog_videos(
        self,
        urls: List[str],
        run_config: CSJWebScrapeConfig,
    ):
        data = {
            "base_url": urls,
            "video_name": [[] for _ in urls],
            "video_link": [[] for _ in urls],
        }
        async with AsyncWebCrawler(config=run_config.browser_config) as crawler:
            try:
                if self.method == "series":
                    # scrape
                    results = [
                        await crawler.arun(
                            url=url,
                            config=run_config.run_config_init_bsf,
                        )
                        for url in urls
                    ]
                    # flatten `results`
                    temp_data = [
                        item2._results[0] for item1 in results for item2 in item1
                    ]
                    flat_rslt = CrawlResultContainer(temp_data)
                    # extract `results`
                    for idx, url in enumerate(urls):
                        for result in flat_rslt:
                            if url in result.url:
                                for video in json.loads(result.extracted_content)[0][
                                    "articles"
                                ]:
                                    data["video_name"][idx].append(video["video_name"])
                                    data["video_link"][idx].append(video["video_link"])

                    return data

                elif self.method == "parallel":  # Method not working. Don't call this
                    # scrape                    # bug in crawl4ai. check ["https://github.com/unclecode/crawl4ai/issues/1277"]
                    results = await crawler.arun_many(
                        urls=urls,
                        config=run_config.run_config_init_bsf,
                        dispatcher=run_config.mem_ada_dispatcher,  # <- issues
                    )
                    return {}

            except Exception as e:
                LogException(e, "Extract", log_etl)
                # return {}
                raise CustomException(e)

    async def _scrape_transcripts(
        self,
        data: dict,
        run_config: CSJWebScrapeConfig,
    ):
        data["video_transcript"] = [[] for _ in data["base_url"]]
        async with AsyncWebCrawler(config=run_config.browser_config) as crawler:
            try:
                for idx, urls in enumerate(data["video_link"]):
                    # get data
                    results = await crawler.arun_many(
                        urls=urls,
                        config=run_config.run_config_tran,
                        dispatcher=run_config.mem_ada_dispatcher,
                    )

                    # flatten `results`
                    temp_data = [item1._results[0] for item1 in results]
                    results = CrawlResultContainer(temp_data)

                    # reorder data
                    results_ordr = [
                        next(r for r in results if r.url == url) for url in urls
                    ]

                    # append data
                    for result in results_ordr:
                        for sp_data in json.loads(result.extracted_content):
                            data["video_transcript"][idx].append(sp_data["transcript"])

                return data

            except Exception as e:
                LogException(e, "Extract", log_etl)
                raise CustomException(e)

    def _save(self, i, j, data, video_url, trscps, save_dir):
        try:
            file_name = f"{self.data['KEY'][i]}E{j + 1:02d}-{data['video_name'][i][j]} | CS Joseph.txt"
            log_etl.info(f"Extract: Saving '{file_name}'")

            # make save folder
            if not os.path.exists(save_dir):
                os.makedirs(save_dir, exist_ok=True)

            # write data
            file_path = os.path.join(save_dir, file_name)

            # prep transcript
            trscps = "\n".join(textwrap.wrap(trscps, width=160))

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(f"{file_name[:-4]}\n\n")
                f.write(f"{video_url}\n\n")
                f.write(trscps)

        except Exception as e:
            LogException(e, "Extract", log_etl)
            raise CustomException(e)

    def _save_transcripts(self, data: dict):
        try:
            for i in range(len(self.data["URL"])):
                save_dir = f"{RawData.RAW_CSJ_BLOG}/{self.data['NAME'][i]}/"
                video_list = data["video_link"][i]
                trscp_list = data["video_transcript"][i]

                # Process videos in parallel with max 10 threads
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = []
                    for j, (video_url, trscps) in enumerate(
                        zip(video_list, trscp_list)
                    ):
                        futures.append(
                            executor.submit(
                                self._save, i, j, data, video_url, trscps, save_dir
                            )
                        )

                    # Wait for all futures to complete
                    for future in as_completed(futures):
                        pass

        except Exception as e:
            LogException(e, "Extract", log_etl)
            raise CustomException(e)

    async def run(self):
        try:
            log_etl.info("Extract: Blog video transcript scraping started")
            urls_csj = [url for url in self.urls if "csjoseph.life" in url]

            log_etl.info("Extract: Deep crawling for individual videos per season")
            data_csj = await self._process_blog_videos(
                urls=urls_csj,
                run_config=self.crw_csj_config,
            )

            log_etl.info("Extract: Scraping transcripts from blog")
            trnc_csj = await self._scrape_transcripts(
                data=data_csj,
                run_config=self.crw_csj_config,
            )

            log_etl.info("Extract: Saving transcripts to file")
            self._save_transcripts(data=trnc_csj)

            log_etl.info("Extract: Blog video transcript data was saved")
        except Exception as e:
            LogException(e, "Extract", log_etl)
            raise CustomException(e)


class SkoolTranscriptWriter:
    """Extract `paid video contents` transcript from Skool."""

    def __init__(self) -> None:
        pass

    def run(self):
        try:
            ...
            pass

        except Exception as e:
            LogException(e, "Extract", log_etl)
            raise CustomException(e)
