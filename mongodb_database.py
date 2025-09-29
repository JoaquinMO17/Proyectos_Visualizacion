from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

class MongoDB:
    client: Optional[AsyncIOMotorClient] = None
    sync_client: Optional[MongoClient] = None
    database = None
    sync_database = None

# Global instance
mongodb = MongoDB()

async def connect_to_mongo():
    """
    Create asynchronous connection to MongoDB
    """
    MONGO_URL = "mongodb://admin:admin123@mongodb:27017/movies_db?authSource=admin"
    
    mongodb.client = AsyncIOMotorClient(MONGO_URL)
    mongodb.database = mongodb.client.movies_db
    
    # Verify connection
    try:
        await mongodb.client.admin.command('ping')
        print("Successfully connected to MongoDB (async)")
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")

def connect_to_mongo_sync():
    """
    Create synchronous connection to MongoDB
    """
    MONGO_URL = "mongodb://admin:admin123@mongodb:27017/movies_db?authSource=admin"
    
    mongodb.sync_client = MongoClient(MONGO_URL)
    mongodb.sync_database = mongodb.sync_client.movies_db
    
    # Verify connection
    try:
        mongodb.sync_client.admin.command('ping')
        print("Successfully connected to MongoDB (sync)")
        return True
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return False

async def close_mongo_connection():
    """
    Close MongoDB connection
    """
    if mongodb.client:
        mongodb.client.close()
        print("MongoDB async connection closed")
    
    if mongodb.sync_client:
        mongodb.sync_client.close()
        print("MongoDB sync connection closed")

def get_mongo_database():
    """
    Get async database reference
    """
    return mongodb.database

def get_mongo_database_sync():
    """
    Get sync database reference for use in synchronous code
    """
    if not mongodb.sync_database:
        connect_to_mongo_sync()
    return mongodb.sync_database