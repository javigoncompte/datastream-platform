from typing import Any, Dict, Type, TypeVar

from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import DeclarativeBase, registry

from unity_orm.logger import get_logger

T = TypeVar("T", bound="ManagedTable")
logger = get_logger(__name__)


class ManagedTable(DeclarativeBase):
    metadata = MetaData()
    registry = registry()

    @declared_attr.directive
    @classmethod
    def __tablename__(cls) -> str:
        name = cls.__name__
        return "".join([
            "_" + char.lower() if char.isupper() else char for char in name
        ]).lstrip("_")

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for key in self.__mapper__.c.keys():
            result[key] = getattr(self, key)
        return result
