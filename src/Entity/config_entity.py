import os
from dotenv import load_dotenv

from src.Constants import mongo_db_dc


class MongoDBConfig:
    def __init__(self):
        load_dotenv("src/Secrets/Secrets.env")
        MONGO_DB_UN = os.getenv("MONGO_DB_UN")
        MONGO_DB_PW = os.getenv("MONGO_DB_PW")
        self.mongo_db_url = f"mongodb+srv://{MONGO_DB_UN}:{MONGO_DB_PW}@cluster0.8y5aipc.mongodb.net/?retryWrites=true&w=majority&appName={mongo_db_dc.CLUSTER_NAME}"
        self.database = mongo_db_dc.DATABASE_NAME
        self.collection_yutu = mongo_db_dc.COLLECTION_NAME_YUTU
        self.collection_blog = mongo_db_dc.COLLECTION_NAME_BLOG
