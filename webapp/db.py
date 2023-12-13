from pymongo.mongo_client import MongoClient
from config import URI, DB


def connect_db():
    """Create a client connection to MongoDB"""
    try:
        client = MongoClient(URI)
        mongodb = client[DB]

        return mongodb
    except Exception as e:
        print(e)


def insert(items):
    "Insert data to mongoDB"

    mongodb = connect_db()
    collection = mongodb["vehicle_position"]

    collection.insert_many(items.to_dict("records"))