import pytest
import mongomock
from file_flow_common import db


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
