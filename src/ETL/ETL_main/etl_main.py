import os
from pytube import Playlist, YouTube
from youtube_transcript_api import YouTubeTranscriptApi
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.ETL.ETL_config import MetadataConfig, ProxyConfig
from src.ETL.ETL_constants import RawData

from src.Logging.logger import log_etl
from src.Exception.exception import CustomException, LogException


class TranscriptWriter:
    def __init__(
        self,
        metadata: MetadataConfig | None = MetadataConfig(),
        proxy_rotation_config: ProxyConfig | None = ProxyConfig(),
    ) -> None:
        self.metadata = metadata
        self.proxy_config = (
            proxy_rotation_config.proxy_config if proxy_rotation_config else None
        )
        self.data = self.metadata.df

    def process_video(self, key, j, video_url, save_folder):
        try:
            # get video details
            yt = YouTube(video_url)
            video_name = yt.title.replace("/", " & ")
            file_name = f"{key}E{j + 1:02d}-{video_name}.txt"
            log_etl.info(f"Processing video: {file_name}")

            # get video transcript
            yt_ts_api = YouTubeTranscriptApi(proxy_config=self.proxy_config)
            transcript_list = yt_ts_api.list(yt.video_id)
            transcript = transcript_list.find_transcript(["en"])
            video_transcript = transcript.fetch()
            transcript_text = " ".join([snippet.text for snippet in video_transcript])

            # make save folder
            if not os.path.exists(save_folder):
                os.makedirs(save_folder)

            # write data
            # file_name = f"{key}E{j + 1:02d}-{video_name}.txt"
            file_path = os.path.join(save_folder, file_name)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(f"{key}E{j + 1:02d}: {video_name}\n\n")
                f.write(f"{video_url}\n\n")
                f.write(transcript_text)

            log_etl.info(f"Saved video: {file_name}")

        except Exception as e:
            LogException(e, "ETL", log_etl)
            raise CustomException(e)

    def run(self):
        try:
            self.data.fillna("", inplace=True)
            my_dict = self.data.to_dict(orient="index")

            for i, (key, value) in enumerate(my_dict.items()):
                if value["URL"]:
                    pl = Playlist(value["URL"])
                    vl = pl.video_urls

                    log_etl.info(f"Processing playlist: {value['NAME']}")
                    save_dir = f"{RawData.RAW_CSJ_FREE}/{value['NAME']}"

                    # Process videos in parallel with max 10 threads
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        futures = []
                        for j, video_url in enumerate(vl):
                            futures.append(
                                executor.submit(
                                    self.process_video, key, j, video_url, save_dir
                                )
                            )

                        # Wait for all futures to complete
                        for future in as_completed(futures):
                            pass  # Results and exceptions handled inside process_video

        except Exception as e:
            LogException(e, "ETL", log_etl)
            raise CustomException(e)
