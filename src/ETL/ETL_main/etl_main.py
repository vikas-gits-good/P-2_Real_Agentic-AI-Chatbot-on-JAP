import os
import textwrap
from glob import glob
from pytube import Playlist, YouTube
from youtube_transcript_api import YouTubeTranscriptApi
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.ETL.ETL_constants import RawData
from src.ETL.ETL_config import MetadataConfig, ProxyConfig

from src.Logging.logger import log_etl
from src.Exception.exception import CustomException, LogException


class YouTubeTranscriptWriter:
    """Extract free contents' transcript from youtube."""

    def __init__(
        self,
        metadata: MetadataConfig = MetadataConfig(),
        proxy_rotation_config: ProxyConfig = ProxyConfig(),
    ) -> None:
        self.metadata = metadata
        self.proxy_config = proxy_rotation_config.proxy_config
        self.df_full = self.metadata.df_full
        self.full_files = self._check_download()

    def _check_download(self):
        try:
            log_etl.info("Extract: Checking files to skip downloading")
            # text files that are present locally
            files_csj = glob(f"{self.metadata.raw_csj_free_path}/**/*.txt")
            files_rp = glob(f"{self.metadata.raw_rp_free_path}/**/*.txt")
            files_local = files_csj + files_rp

            # all transcripts that are available
            full_dict = self.df_full.to_dict(orient="index")
            files_full = {"pl_url": [], "sv_path": [], "vd_url": [], "vid_name": []}
            for idx, value in full_dict.items():
                log_etl.info(
                    f"Extract: Analysing playlist {idx + 1:02d} ('{value['KEY']}') of {self.df_full.shape[0]} -> '{value['NAME']}'"
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

            # log_etl.info(f"\n{files_full}") # <- Working fine
            # filter out missing files
            for playlist_idx, (videos, titles) in enumerate(
                zip(files_full["vd_url"], files_full["vid_name"])
            ):
                video_list = list(videos)  # Convert to regular list
                title_list = list(titles)

                # Filter both lists simultaneously
                keep_indices = []
                for i, title in enumerate(title_list):
                    titl_chk = title[:9]
                    is_present = False
                    for item in files_local:
                        item_chk = item.split("/")[-1][:9]
                        if item_chk == titl_chk:
                            is_present = True
                            break
                    if not is_present:
                        log_etl.info(f"Missing: {item_chk}")
                        keep_indices.append(i)

                # Keep only non-matching items
                files_full["vd_url"][playlist_idx] = [
                    video_list[i] for i in keep_indices
                ]
                files_full["vid_name"][playlist_idx] = [
                    title_list[i] for i in keep_indices
                ]

            # log_etl.info(f"\n{files_full}")

            # clean up the data
            for i in range(len(files_full["vd_url"]) - 1, -1, -1):
                if len(files_full["vd_url"][i]) == 0:
                    files_full["pl_url"].remove(files_full["pl_url"][i])
                    files_full["sv_path"].remove(files_full["sv_path"][i])
                    files_full["vd_url"].remove(files_full["vd_url"][i])
                    files_full["vid_name"].remove(files_full["vid_name"][i])

            log_etl.info("Extract: Exporting filtered files to download")
            # log_etl.info(f"\n{files_full}")
            return files_full

        except Exception as e:
            LogException(e, "Error", log_etl)
            raise CustomException(e)

    def process_video(self, i, j, video_url, save_folder):
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
                os.makedirs(save_folder)

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
                                self.process_video, i, j, video_url, save_dir
                            )
                        )

                    # Wait for all futures to complete
                    for future in as_completed(futures):
                        pass  # Results and exceptions handled inside process_video

        except Exception as e:
            LogException(e, "Extract", log_etl)
            raise CustomException(e)


class SkoolTranscriptWriter:
    """Extract paid contents' transcript from Skool."""

    def __init__(self) -> None:
        pass

    def run(self):
        try:
            ...
            pass

        except Exception as e:
            LogException(e, "Extract", log_etl)
            raise CustomException(e)
