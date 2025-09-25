import pytest
import mongomock
from file_flow_common import db
from datetime import datetime, timedelta


@pytest.fixture(autouse=True)
def mock_mongo(monkeypatch):
    """Patch db.Mongo to use an in-memory mongomock client."""
    client = mongomock.MongoClient()

    monkeypatch.setattr(db.Mongo, "get_db", lambda: client["test_db"])
    monkeypatch.setattr(
        db.Mongo, "get_collection", lambda name: client["test_db"][name]
    )

    yield
    client.drop_database("test_db")


def test_insert_and_get_document():
    data = {"base64": "/9j/4AAQSkZJRgABAQAAAQABAAD..."}
    doc_id = db.insert_document("images", data)

    fetched = db.get_document("images", doc_id)

    assert fetched is not None
    assert fetched["id"] == doc_id
    assert fetched["base64"] == data["base64"]


def test_upsert_document_inserts_and_updates():
    coll = db.Mongo.get_collection("filtered_images")

    doc_id = "test123"

    # 1. First upsert should insert
    inserted = db.upsert_document(
        "filtered_images",
        doc_id=doc_id,
        data={"filterA": "imgA"},
    )
    assert inserted is True
    doc = coll.find_one({"id": doc_id})
    assert doc is not None
    assert doc["filterA"] == "imgA"

    # 2. Second upsert should update the same document
    updated = db.upsert_document(
        "filtered_images",
        doc_id=doc_id,
        data={"filterB": "imgB"},
    )
    assert updated is True
    doc = coll.find_one({"id": doc_id})
    assert doc["filterA"] == "imgA"  # original field remains
    assert doc["filterB"] == "imgB"  # new field added


def test_update_document():
    doc_id = db.insert_document("images", {"base64": "old"})
    ok = db.update_document("images", doc_id, {"base64": "new"})

    assert ok is True
    fetched = db.get_document("images", doc_id)
    assert fetched["base64"] == "new"


def test_delete_document():
    doc_id = db.insert_document("images", {"base64": "to-delete"})
    ok = db.delete_document("images", doc_id)

    assert ok is True
    assert db.get_document("images", doc_id) is None


def test_delete_older_than_n_days():
    days = 1
    old_ts = int((datetime.now() - timedelta(days=days + 1)).timestamp())

    db.insert_document("images", {"base64": "old", "createdAt": old_ts})

    deleted = db.delete_documents_older_than_ts(collection="images", days=days)

    assert deleted == 1
