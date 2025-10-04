from datetime import datetime

from pydantic import BaseModel
from sqlalchemy.engine import Engine
from sqlmodel import Field, SQLModel, create_engine

from dataplatform.evalgen.databricks_integration.db import get_uri

CATALOG = "agents"
SCHEMA = "evalgen"


class Trace(BaseModel):
    trace: str
    trace_id: str


class Document(BaseModel):
    doc_uri: str
    content: str


class HumanSource(BaseModel):
    user_name: str


class Source(BaseModel):
    human: HumanSource
    trace: Trace
    document: Document
    tags: dict[str, str]


class DatabricksDatasetModel(SQLModel, table=True):
    dataset_record_id: int = Field(default=None, primary_key=True)
    inputs: str
    expectations: str
    create_time: datetime = Field(default_factory=datetime.now)
    created_by: str
    last_updated_time: datetime = Field(default_factory=datetime.now)
    last_updated_by: str
    source: Source


ENGINE: Engine = create_engine(get_uri(CATALOG, SCHEMA), echo=True)
SQLModel.metadata.create_all(ENGINE)
