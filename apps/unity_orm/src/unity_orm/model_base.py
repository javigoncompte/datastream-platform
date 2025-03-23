"""Base classes for Databricks ORM models."""

from typing import Any, Dict, Type, TypeVar

from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import DeclarativeBase, registry

from unity_orm.logger import get_logger

T = TypeVar("T", bound="ManagedTable")
logger = get_logger(__name__)


class ManagedTable(DeclarativeBase):
    """Base class for all Unity ORM models.

    This class provides common functionality for all ORM models
    including automatic table naming and metadata management.
    """

    # Class variable to store metadata about the model
    metadata = MetaData()

    # Registry for managing ORM mappings
    registry = registry()

    # Table naming convention based on class name
    @declared_attr.directive
    @classmethod
    def __tablename__(cls) -> str:
        """Generate table name from class name.

        Returns:
            A snake_case table name derived from the class name.
        """
        # Convert CamelCase to snake_case
        name = cls.__name__
        return "".join([
            "_" + char.lower() if char.isupper() else char for char in name
        ]).lstrip("_")

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """Create an instance from a dictionary.

        Args:
            data: Dictionary containing model attributes.

        Returns:
            An instance of the model.
        """
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the model to a dictionary.

        Returns:
            A dictionary representation of the model.
        """
        result: Dict[str, Any] = {}
        for key in self.__mapper__.c.keys():
            result[key] = getattr(self, key)
        return result
