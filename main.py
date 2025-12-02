import asyncio
from src.ETL.ETL_main import YouTubeTranscriptWriter, BlogTranscriptWriter

if __name__ == "__main__":
    # get youtube transcripts
    YouTubeTranscriptWriter().run()

    # get blog transcripts
    asyncio.run(BlogTranscriptWriter().run())
