from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv
from Logger import Logger

load_dotenv()


class Database:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.client = None
        self.db = None
        self.queue_collection = None
        self._initialized = True

    async def connect(self):
        """Connect to MongoDB database"""
        if not self.client:
            try:
                self.client = AsyncIOMotorClient(os.getenv('MONGO_URI'), serverSelectionTimeoutMS=10000)
                await self.client.server_info()
                self.db = self.client['santosh-companies']
                self.queue_collection = self.db['queue3']
                await self.queue_collection.create_index('scraped')

                Logger.info("Successfully connected to MongoDB")
            except Exception as e:
                Logger.error(f"Failed to connect to MongoDB: {str(e)}")

    async def close(self):
        """Close the database connection"""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            self.queue_collection = None
            Logger.info("Closed MongoDB connection")
