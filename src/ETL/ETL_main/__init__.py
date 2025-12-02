import os
import json
import asyncio
import textwrap
import pandas as pd
from typing import Literal
from crawl4ai import AsyncWebCrawler
from crawl4ai.models import CrawlResultContainer
from pytube import Playlist, YouTube
from youtube_transcript_api import YouTubeTranscriptApi
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.ETL.ETL_utils import (
    check_duplicate_videos_manually,
    check_duplicate_blogs_manually,
    check_duplicate_videos_database,
    check_duplicate_blogs_database,
)
from src.ETL.ETL_constants import RawData
from src.ETL.ETL_config import (
    MetadataConfig,
    ProxyConfig,
    CSJWebScrapeConfig,
)

from src.Logging.logger import log_etl
from src.Exception.exception import CustomException, LogException


class YouTubeTranscriptWriter:
    """Extract `free video content` transcript from youtube."""

    def __init__(
        self,
        metadata: MetadataConfig = MetadataConfig(source="video"),
        proxy_rotation_config: ProxyConfig = ProxyConfig(),
        duplicate_search: Literal["database", "manual"] = "database",
    ) -> None:
        self.proxy_config = proxy_rotation_config.proxy_config
        self.df_full = metadata.df_full
        self.part_files = (
            check_duplicate_videos_manually(data=self.df_full)
            if duplicate_search == "manual"
            else check_duplicate_videos_database()
        )

    def _process_video(self, i, j, video_url, save_folder):
        try:
            # get video details
            yt = YouTube(video_url)
            file_name = self.part_files["vid_name"][i][j]
            if file_name:
                log_etl.info(f"Extract: Processing {file_name}")

                # get video transcript
                yt_ts_api = YouTubeTranscriptApi(proxy_config=self.proxy_config)
                transcript_list = yt_ts_api.list(yt.video_id)
                transcript = transcript_list.find_transcript(["en"])
                video_transcript = transcript.fetch()
                transcript_text = " ".join(
                    [snippet.text for snippet in video_transcript]
                )
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
            num_sesn = len([item for item in self.part_files["pl_url"] if item])
            num_vids = len(
                [
                    item
                    for url_list in self.part_files["vd_url"]
                    for item in url_list
                    if item
                ]
            )
            log_etl.info("Extract: Blog video transcript scraping started")
            log_etl.info(f"Filt Data:\n{self.part_files}")
            if not all(item == "" for item in self.part_files["pl_url"]):
                log_etl.info(
                    f"Extract: Scraping: {num_vids:03d} transcripts from {num_sesn:02d} seasons"
                )

                for i in range(len(self.part_files["pl_url"])):
                    pl = Playlist(self.part_files["pl_url"][i])
                    vl = pl.video_urls

                    log_etl.info(
                        f"Extract: Parallel processing '{self.part_files['sv_path'][i]}'"
                    )
                    save_dir = (
                        f"{RawData.RAW_CSJ_FREE}/{self.part_files['sv_path'][i]}/"
                    )

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

            else:
                log_etl.info("Extract: No new data to scrape. Stopping")

        except Exception as e:
            LogException(e, "Extract", log_etl)
            raise CustomException(e)


class BlogTranscriptWriter:
    """Extract `free blog content` transcript from Blog."""

    def __init__(
        self,
        method: Literal["series", "parallel"] = "series",
        duplicate_search: Literal["database", "manual"] = "database",
    ) -> None:
        self.method = method
        self.data: pd.DataFrame = MetadataConfig(source="blog").df_full
        self.data_csj = (
            asyncio.run(check_duplicate_blogs_manually(data=self.data))
            if duplicate_search == "manual"
            else check_duplicate_blogs_database(data=self.data)
        )

    async def _scrape_transcripts(
        self,
        data: dict,
        run_config: CSJWebScrapeConfig,
    ):
        data["video_transcript"] = [
            [""] * len(sublist) for sublist in data["video_name"]
        ]
        """data = {
              'base_url': ['url-1','','url-3','','url-5'],
            'video_name': [['','url-1_vids-2', ''],[],['url-3_vids-1','',''],[],['','','url-5_vids-3']],
            'video_link': [['','url-1_link-2', ''],[],['url-3_link-1','',''],[],['','','url-5_link-3']],
      'video_transcript': [['','', ''],[],['','',''],[],['','','']],
        }            
        """
        async with AsyncWebCrawler(config=run_config.browser_config) as crawler:
            try:
                for i, url_list in enumerate(data["video_link"]):
                    if len(url_list) > 0:  # skip empty list
                        urls = [s for s in url_list if s]
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
                        for j, url in enumerate(url_list):
                            for result in results_ordr:
                                if url == result.url:
                                    trsp = json.loads(result.extracted_content)
                                    trsp = (
                                        trsp[0]
                                        if isinstance(trsp, list) and len(trsp) > 0
                                        else {"transcript": "Transcript not found"}
                                    )
                                    data["video_transcript"][i][j] = trsp["transcript"]

                return data

            except Exception as e:
                LogException(e, "Extract", log_etl)
                raise CustomException(e)

    def _save(self, i, j, data, video_url, trscps, save_dir):
        try:  # skip saved season               # skip saved video
            if len(data["video_name"]) > 0 and data["video_name"][i][j]:
                file_name = f"{self.data['KEY'][i]}E{j + 1:02d}-{data['video_name'][i][j]} | CS Joseph.txt"
                file_name = file_name.replace("/", "&")
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
            for i in range(len(data["base_url"])):
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
            """data_csj = {
                'base_url'  : ['url-1','','url-3','','url-5'],
                'video_name': [['','url-1_vids-2', ''],[],['url-3_vids-1','',''],[],['','','url-5_vids-3']],
                'video_link': [['','url-1_link-2', ''],[],['url-3_link-1','',''],[],['','','url-5_link-3']],
            }
            """
            num_sesn = len([item for item in self.data_csj["base_url"] if item])
            num_vids = len(
                [
                    item
                    for url_list in self.data_csj["video_link"]
                    for item in url_list
                    if item
                ]
            )
            log_etl.info("Extract: Blog video transcript scraping started")
            log_etl.info(f"Filt Data:\n{self.data_csj}")
            if not all(item == "" for item in self.data_csj["base_url"]):
                log_etl.info(
                    f"Extract: Scraping: {num_vids:03d} transcripts from {num_sesn:02d} seasons"
                )
                crw_csj_config = CSJWebScrapeConfig(
                    max_parallel=5, len_list=len(self.data_csj["base_url"])
                )
                trnc_csj = await self._scrape_transcripts(
                    data=self.data_csj,
                    run_config=crw_csj_config,
                )

                log_etl.info("Extract: Saving transcripts to file")
                self._save_transcripts(data=trnc_csj)

                log_etl.info("Extract: Blog video transcript data was saved")

            else:
                log_etl.info("Extract: No new data to scrape. Stopping")

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
