from pymongo import MongoClient
from pymongo.collection import Collection
import uuid
from datetime import datetime, timedelta
from src.schema.query import DbAccessQuery


class Mongo:
    _client: MongoClient | None = None

    @classmethod
    def get_db(cls, mongo_uri: str, db_name: str):
        if cls._client is None:
            cls._client = MongoClient(mongo_uri)
        return cls._client[db_name]

    @classmethod
    def get_collection(cls, db_query: DbAccessQuery) -> Collection:
        return cls.get_db(db_query.mongo_uri, db_query.db_name)[
            db_query.collection_name
        ]

    @classmethod
    def close(cls):
        if cls._client:
            cls._client.close()
            cls._client = None


def insert_document(db_query: DbAccessQuery, data: dict) -> str:
    data["id"] = str(uuid.uuid4())
    data.setdefault("createdAt", datetime.now().timestamp())
    Mongo.get_collection(db_query).insert_one(data)
    return data["id"]


def upsert_document(db_query: DbAccessQuery, doc_id: str, data: dict) -> bool:
    """Insert new doc if not exists, otherwise update fields."""
    result = Mongo.get_collection(db_query).update_one(
        {"id": doc_id},
        {"$set": data},
        upsert=True,
    )
    return result.matched_count > 0 or result.upserted_id is not None


def get_document(db_query: DbAccessQuery, doc_id: str) -> dict | None:
    return Mongo.get_collection(db_query).find_one({"id": doc_id})


def update_document(db_query: DbAccessQuery, doc_id: str, update: dict) -> bool:
    result = Mongo.get_collection(db_query).update_one({"id": doc_id}, {"$set": update})
    return result.modified_count > 0


def delete_document(db_query: DbAccessQuery, doc_id: str) -> bool:
    result = Mongo.get_collection(db_query).delete_one({"id": doc_id})
    return result.deleted_count > 0


def delete_documents_older_than_ts(db_query: DbAccessQuery, days: int = 10) -> bool:
    cutoff = datetime.now() - timedelta(days=days)
    cutoff_ts = int(cutoff.timestamp())
    result = Mongo.get_collection(db_query).delete_many(
        {"createdAt": {"$lte": cutoff_ts}}
    )
    return result.deleted_count
