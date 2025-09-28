from pydantic import BaseModel


class DbAccessQuery(BaseModel):
    mongo_uri: str
    db_name: str
    collection_name: str
