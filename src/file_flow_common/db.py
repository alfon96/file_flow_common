from pymongo import MongoClient
from pymongo.collection import Collection
import os
import uuid
from datetime import datetime, timedelta

MONGO_URI = os.getenv("MONGO_URI", "")
DB_NAME = os.getenv("MONGO_DB", "")


class Mongo:
    _client: MongoClient | None = None

    @classmethod
    def get_db(cls):
        if cls._client is None:
            cls._client = MongoClient(MONGO_URI)
        return cls._client[DB_NAME]

    @classmethod
    def get_collection(cls, name: str) -> Collection:
        return cls.get_db()[name]

    @classmethod
    def close(cls):
        if cls._client:
            cls._client.close()
            cls._client = None


def insert_document(collection: str, data: dict) -> str:
    data["id"] = str(uuid.uuid4())
    data.setdefault("createdAt", datetime.now().timestamp())
    Mongo.get_collection(collection).insert_one(data)
    return data["id"]


def upsert_document(collection: str, doc_id: str, data: dict) -> bool:
    """Insert new doc if not exists, otherwise update fields."""
    result = Mongo.get_collection(collection).update_one(
        {"id": doc_id},
        {"$set": data},
        upsert=True,
    )
    return result.matched_count > 0 or result.upserted_id is not None


def get_document(collection: str, doc_id: str) -> dict | None:
    return Mongo.get_collection(collection).find_one({"id": doc_id})


def update_document(collection: str, doc_id: str, update: dict) -> bool:
    result = Mongo.get_collection(collection).update_one(
        {"id": doc_id}, {"$set": update}
    )
    return result.modified_count > 0


def delete_document(collection: str, doc_id: str) -> bool:
    result = Mongo.get_collection(collection).delete_one({"id": doc_id})
    return result.deleted_count > 0


def delete_documents_older_than_ts(collection: str, days: int = 10) -> bool:
    cutoff = datetime.now() - timedelta(days=days)
    cutoff_ts = int(cutoff.timestamp())
    result = Mongo.get_collection(collection).delete_many(
        {"createdAt": {"$lte": cutoff_ts}}
    )
    return result.deleted_count
