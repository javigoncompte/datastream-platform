"""TableInfo model for Unity ORM generators."""

from typing import Optional

from pydantic import BaseModel


class TableInfo(BaseModel):
    """Information about a database table."""

    catalog: str
    schema: str
    name: str
    comment: Optional[str] = None
