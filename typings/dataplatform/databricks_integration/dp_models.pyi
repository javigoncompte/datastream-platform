"""
This type stub file was generated by pyright.
"""

from datetime import datetime
from pydantic import BaseModel
from sqlalchemy.engine import Engine
from sqlmodel import SQLModel

class Trace(BaseModel):
    trace: str
    trace_id: str
    ...


class Document(BaseModel):
    doc_uri: str
    content: str
    ...


class HumanSource(BaseModel):
    user_name: str
    ...


class Source(BaseModel):
    human: HumanSource
    trace: Trace
    document: Document
    tags: dict[str, str]
    ...


class DatabricksDatasetModel(SQLModel, table=True):
    dataset_record_id: int = ...
    inputs: str
    expectations: str
    create_time: datetime = ...
    created_by: str
    last_updated_time: datetime = ...
    last_updated_by: str
    source: Source


ENGINE: Engine = ...
