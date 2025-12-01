import asyncio
from src.ETL.ETL_main import BlogTranscriptWriter

if __name__ == "__main__":
    asyncio.run(BlogTranscriptWriter().run())
