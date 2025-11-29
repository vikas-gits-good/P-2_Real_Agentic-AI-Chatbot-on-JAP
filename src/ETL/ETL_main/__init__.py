import os
import textwrap
from typing import Literal
from pytube import Playlist, YouTube
from youtube_transcript_api import YouTubeTranscriptApi
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.ETL.ETL_utils import check_duplicates
from src.ETL.ETL_constants import RawData
from src.ETL.ETL_config import MetadataConfig, ProxyConfig

from src.Logging.logger import log_etl
from src.Exception.exception import CustomException, LogException


class YouTubeTranscriptWriter:
    """Extract `free video content` transcript from youtube."""

    def __init__(
        self,
        metadata: MetadataConfig = MetadataConfig(source="video"),
        proxy_rotation_config: ProxyConfig = ProxyConfig(provider="WebShare"),
    ) -> None:
        self.metadata = metadata
        self.proxy_config = proxy_rotation_config.proxy_config
        self.df_full = self.metadata.df_full
        self.full_files = check_duplicates(data=self.df_full)

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


class BlogTranscriptWriter:
    """Extract `free blog content` transcript from Blog."""

    def __init__(
        self,
        source: Literal["CSJ", "RP"] = "CSJ",
        content: Literal["Video", "Article"] = "Video",
    ) -> None:
        self.source = source
        self.content = content

    def run(self):
        try:
            ...
            pass

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
