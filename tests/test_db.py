import pytest
import mongomock
from file_flow_common import db
from datetime import datetime, timedelta
from src.schema.query import DbAccessQuery

TEST_QUERY = DbAccessQuery(
    mongo_uri="test_uri",
    db_name="test_db_name",
    collection_name="test_collection_name",
)


@pytest.fixture(autouse=True)
def mock_mongo(monkeypatch):
    """Patch db.Mongo to use an in-memory mongomock client with DbAccessQuery."""
    client = mongomock.MongoClient()
    test_db_name = "test_db"

    def mock_get_db(mongo_uri: str, db_name: str):
        return client[db_name]

    def mock_get_collection(db_query: DbAccessQuery):
        return client[db_query.db_name][db_query.collection_name]

    monkeypatch.setattr(db.Mongo, "get_db", mock_get_db)
    monkeypatch.setattr(db.Mongo, "get_collection", mock_get_collection)

    yield

    client.drop_database(test_db_name)


def test_insert_and_get_document():
    data = {"base64": "/9j/4AAQSkZJRgABAQAAAQABAAD..."}

    doc_id = db.insert_document(db_query=TEST_QUERY, data=data)
    fetched = db.get_document(db_query=TEST_QUERY, doc_id=doc_id)

    assert fetched is not None
    assert fetched["id"] == doc_id
    assert fetched["base64"] == data["base64"]


def test_upsert_document_inserts_and_updates():
    coll = db.Mongo.get_collection(db_query=TEST_QUERY)

    doc_id = "test123"

    # 1. First upsert should insert
    inserted = db.upsert_document(
        db_query=TEST_QUERY,
        doc_id=doc_id,
        data={"filterA": "imgA"},
    )
    assert inserted is True
    doc = coll.find_one({"id": doc_id})
    assert doc is not None
    assert doc["filterA"] == "imgA"

    # 2. Second upsert should update the same document
    updated = db.upsert_document(
        db_query=TEST_QUERY,
        doc_id=doc_id,
        data={"filterB": "imgB"},
    )
    assert updated is True
    doc = coll.find_one({"id": doc_id})
    assert doc["filterA"] == "imgA"  # original field remains
    assert doc["filterB"] == "imgB"  # new field added


def test_update_document():
    doc_id = db.insert_document(db_query=TEST_QUERY, data={"base64": "old"})
    ok = db.update_document(
        db_query=TEST_QUERY, doc_id=doc_id, update={"base64": "new"}
    )

    assert ok is True
    fetched = db.get_document(db_query=TEST_QUERY, doc_id=doc_id)
    assert fetched["base64"] == "new"


def test_delete_document():
    doc_id = db.insert_document(db_query=TEST_QUERY, data={"base64": "to-delete"})
    ok = db.delete_document(db_query=TEST_QUERY, doc_id=doc_id)

    assert ok is True
    assert db.get_document(db_query=TEST_QUERY, doc_id=doc_id) is None


def test_delete_older_than_n_days():
    days = 1
    old_ts = int((datetime.now() - timedelta(days=days + 1)).timestamp())

    db.insert_document(db_query=TEST_QUERY, data={"base64": "old", "createdAt": old_ts})

    deleted = db.delete_documents_older_than_ts(db_query=TEST_QUERY, days=days)

    assert deleted == 1
