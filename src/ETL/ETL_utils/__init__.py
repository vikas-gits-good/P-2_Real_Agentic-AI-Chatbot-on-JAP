import json
import pandas as pd
from glob import glob
from typing import List, Literal
from pytube import Playlist, YouTube
from crawl4ai import AsyncWebCrawler
from crawl4ai.models import CrawlResultContainer

from src.ETL.ETL_constants import RawData
from src.ETL.ETL_config import CSJWebScrapeConfig

from src.Logging.logger import log_etl
from src.Exception.exception import LogException, CustomException


def check_duplicate_videos(data: pd.DataFrame) -> dict:
    try:
        log_etl.info("Extract: Checking files to skip downloading")
        # text files that are present locally
        files_csj = glob(f"{RawData.RAW_CSJ_FREE}/**/*.txt")
        files_rp = glob(f"{RawData.RAW_RP_FREE}/**/*.txt")
        files_local = files_csj + files_rp

        # all transcripts that are available
        full_dict = data.to_dict(orient="index")
        files_full = {"pl_url": [], "sv_path": [], "vd_url": [], "vid_name": []}
        for idx, value in full_dict.items():
            log_etl.info(
                f"Extract: Analysing playlist {idx + 1:02d} ('{value['KEY']}') of {data.shape[0]} -> '{value['NAME']}'"
            )
            pl = Playlist(value["URL"])
            video_list = pl.video_urls
            files_full["pl_url"].append(value["URL"])
            files_full["sv_path"].append(value["NAME"])
            files_full["vd_url"].append(video_list)
            name_list = []
            for j, vid in enumerate(video_list):
                yt = YouTube(vid)
                video_name = yt.title.replace("/", " & ")
                file_name = f"{value['KEY']}E{j + 1:02d}-{video_name}.txt"
                name_list.append(file_name)
            files_full["vid_name"].append(name_list)

        # filter out missing files
        for playlist_idx, (videos, titles) in enumerate(
            zip(files_full["vd_url"], files_full["vid_name"])
        ):
            playlist_list = list(files_full["pl_url"])
            name_list = list(files_full["sv_path"])
            video_list = list(videos)  # Convert to regular list
            title_list = list(titles)

            # Filter both lists simultaneously
            keep_indices = []
            keep_playlist = set()
            for i, title in enumerate(title_list):
                titl_chk = title[:9]
                is_present = False
                for item in files_local:
                    item_chk = item.split("/")[-1][:9]
                    if item_chk == titl_chk:
                        is_present = True
                        break
                if not is_present:
                    keep_playlist.add(playlist_idx)
                    keep_indices.append(i)

            # Keep only non-matching items
            files_full["pl_url"][playlist_idx] = [
                playlist_list[i] for i in keep_playlist
            ]
            files_full["sv_path"][playlist_idx] = [name_list[i] for i in keep_playlist]
            files_full["vd_url"][playlist_idx] = [video_list[i] for i in keep_indices]
            files_full["vid_name"][playlist_idx] = [title_list[i] for i in keep_indices]

        # log_etl.info(f"\n{files_full}")

        # clean up the data
        for i in range(len(files_full["vd_url"]) - 1, -1, -1):
            if len(files_full["vd_url"][i]) == 0:
                # log_etl.info(f"index {i}, removing: {files_full["vd_url"][i] = }")
                files_full["sv_path"].remove(files_full["sv_path"][i])
                files_full["pl_url"].remove(files_full["pl_url"][i])
                files_full["vd_url"].remove(files_full["vd_url"][i])
                files_full["vid_name"].remove(files_full["vid_name"][i])

        pl = []
        nm = []
        for playlists, names in zip(files_full["pl_url"], files_full["sv_path"]):
            for playlist, name in zip(playlists, names):
                pl.append(playlist)
                nm.append(name)
        files_full["pl_url"] = pl
        files_full["sv_path"] = nm

        log_etl.info(f"Extract: Finalised sources to download:\n{files_full}")
        return files_full

    except Exception as e:
        LogException(e, "Error", log_etl)
        raise CustomException(e)


async def check_duplicate_blogs(data: pd.DataFrame) -> dict:
    try:
        log_etl.info("Extract: Checking files to skip downloading")
        files_csj = glob(f"{RawData.RAW_CSJ_BLOG}/**/*.txt")
        files_rp = glob(f"{RawData.RAW_RP_BLOG}/**/*.txt")
        files_local = files_csj + files_rp

        urls = data["URL"].to_list()
        urls_csj = [url for url in urls if "csjoseph.life" in url]

        data_to_scrape = await process_blog_videos(urls_csj)

        to_remove_outer = []
        for i in range(len(data_to_scrape["base_url"])):
            to_remove_inner = []
            for j, video_name in enumerate(data_to_scrape["video_name"][i]):
                file_name = f"{data['KEY'][i]}E{j + 1:02d}-{video_name} | CS Joseph.txt"
                for files in files_local:
                    if file_name in files:
                        # log_etl.info(f"Extract: Duplicate found: '{file_name}'")
                        to_remove_inner.append(j)

            for j in sorted(to_remove_inner, reverse=True):
                data_to_scrape["video_name"][i][j] = ""
                data_to_scrape["video_link"][i][j] = ""

            if all(item == "" for item in data_to_scrape["video_name"][i]):
                to_remove_outer.append(i)

        # Clean up outer lists
        for i in sorted(to_remove_outer, reverse=True):
            data_to_scrape["base_url"][i] = ""
            data_to_scrape["video_name"][i] = []
            data_to_scrape["video_link"][i] = []

        return data_to_scrape

    except Exception as e:
        LogException(e, "Error", log_etl)
        raise CustomException(e)


async def process_blog_videos(
    urls: List[str],
    method: Literal["series", "parallel"] = "series",
    run_config: CSJWebScrapeConfig = CSJWebScrapeConfig(),
):
    data = {
        "base_url": urls,
        "video_name": [[] for _ in urls],
        "video_link": [[] for _ in urls],
    }
    async with AsyncWebCrawler(config=run_config.browser_config) as crawler:
        try:
            if method == "series":
                # scrape
                results = [
                    await crawler.arun(
                        url=url,
                        config=run_config.run_config_init_bsf,
                    )
                    for url in urls
                ]
                # flatten `results`
                temp_data = [item2._results[0] for item1 in results for item2 in item1]
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

            elif method == "parallel":  # Method not working. Don't call this
                # scrape                # bug in crawl4ai.
                results = await crawler.arun_many(  # check ["https://github.com/unclecode/crawl4ai/issues/1277"]
                    urls=urls,
                    config=run_config.run_config_init_bsf,
                    dispatcher=run_config.mem_ada_dispatcher,  # <- issues
                )
                return {}

        except Exception as e:
            LogException(e, "Extract", log_etl)
            # return {}
            raise CustomException(e)
