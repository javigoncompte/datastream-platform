from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import cached_property
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession


class TablePropertyType(Enum):
    """Types of table properties."""

    BOOLEAN = "boolean"
    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"


@dataclass
class TableProperty:
    """Represents a table property with type validation."""

    key: str
    value: bool | int | float | str
    property_type: TablePropertyType

    def __post_init__(self):
        if self.property_type == TablePropertyType.BOOLEAN and not isinstance(
            self.value, bool
        ):
            raise ValueError(f"Property {self.key} must be boolean")
        elif self.property_type == TablePropertyType.INTEGER and not isinstance(
            self.value, int
        ):
            raise ValueError(f"Property {self.key} must be integer")
        elif self.property_type == TablePropertyType.DECIMAL and not isinstance(
            self.value, (int, float)
        ):
            raise ValueError(f"Property {self.key} must be decimal")


@dataclass
class TableMetadata:
    """Metadata for a Delta table including schema, constraints, and tracking fields."""

    table_name: str
    catalog: str
    schema: str
    table: str
    primary_keys: list[str]
    constraints: dict[str, Any]
    last_modified_field: str | None
    created_date_field: str | None
    is_deleted_field: str | None
    tags: list[str]
    ingestion_pattern: str | None
    location: str
    last_update: datetime | None
    last_operation: str | None
    operation_metrics: dict[str, Any] | None
    properties: dict[str, TableProperty] = field(default_factory=dict)


class DeltaMetadataProvider:
    """Provider for Delta table metadata and schema information with property management."""

    RESERVED_PROPERTIES: set[str] = {"external", "location", "owner", "provider"}
    INGESTION_PATTERN_KEY: str = "dataplatform.ingestion.pattern"

    COMMON_PROPERTIES: dict[str, TablePropertyType] = {
        "delta.appendOnly": TablePropertyType.BOOLEAN,
        "delta.dataSkippingNumIndexedCols": TablePropertyType.INTEGER,
        "delta.deletedFileRetentionDuration": TablePropertyType.STRING,
        "delta.logRetentionDuration": TablePropertyType.STRING,
        "delta.minReaderVersion": TablePropertyType.INTEGER,
        "delta.minWriterVersion": TablePropertyType.INTEGER,
        INGESTION_PATTERN_KEY: TablePropertyType.STRING,
    }

    def __init__(self, spark: SparkSession, table_name: str):
        self.spark: SparkSession = spark
        self.table_name: str = table_name
        self._metadata: TableMetadata | None = None
        self._delta_table: DeltaTable = DeltaTable.forName(spark, table_name)

    @cached_property
    def metadata(self) -> TableMetadata:
        """Get comprehensive table metadata."""
        if self._metadata is None:
            self._metadata = TableMetadata(
                table_name=self.table_name,
                catalog=self._get_catalog(),
                schema=self._get_schema(),
                table=self._get_table(),
                primary_keys=self._get_primary_keys(),
                constraints=self._get_constraints(),
                last_modified_field=self._get_tagged_column("last_modified_field"),
                created_date_field=self._get_tagged_column("created_date_field"),
                is_deleted_field=self._get_tagged_column("is_deleted_field"),
                tags=self._get_tags(),
                ingestion_pattern=self._get_ingestion_pattern(),
                location=self._get_location(),
                last_update=self._get_last_update(),
                last_operation=self._get_last_operation(),
                operation_metrics=self._get_operation_metrics(),
                properties=self._get_table_properties(),
            )
        return self._metadata

    def _get_catalog(self) -> str:
        """Get catalog name."""
        return self.table_name.split(".")[0]

    def _get_schema(self) -> str:
        """Get schema name."""
        return self.table_name.split(".")[1]

    def _get_table(self) -> str:
        """Get table name."""
        return self.table_name.split(".")[-1]

    def _get_primary_keys(self) -> list[str]:
        """Get primary key columns from information schema."""
        df = self.spark.sql(f"""
            SELECT column_name
            FROM system.information_schema.key_column_usage AS cu
            JOIN system.information_schema.table_constraints AS tc
                USING (constraint_catalog, constraint_schema, constraint_name)
            WHERE cu.table_name = '{self._get_table().lower()}'
                AND tc.constraint_type = 'PRIMARY KEY'
                AND cu.table_catalog = '{self._get_catalog()}'
                AND cu.table_schema = '{self._get_schema()}'
            ORDER BY ordinal_position
        """)
        return [row.column_name for row in df.collect()]

    def _get_tagged_column(self, tag_name: str) -> str | None:
        """Get tracking field by tag name."""
        df = self.spark.sql(f"""
            SELECT column_name
            FROM system.information_schema.column_tags
            WHERE table_name = '{self._get_table().lower()}'
                AND tag_name = '{tag_name}'
                AND catalog_name = '{self._get_catalog()}'
                AND schema_name = '{self._get_schema()}'
        """)
        row = df.first()
        return row.column_name if row else None

    def _get_table_properties(self) -> dict[str, TableProperty]:
        """Get all table properties."""
        df = self.spark.sql(f"SHOW TBLPROPERTIES {self.table_name}")
        properties = {}
        for row in df.collect():
            key = row.key
            if not key.startswith("option.") and key not in self.RESERVED_PROPERTIES:
                prop_type = self.COMMON_PROPERTIES.get(key, TablePropertyType.STRING)
                properties[key] = TableProperty(
                    key=key, value=row.value, property_type=prop_type
                )
        return properties

    def set_property(
        self, key: str, value: Any, property_type: TablePropertyType | None = None
    ) -> None:
        """Set a table property."""
        if key in self.RESERVED_PROPERTIES:
            raise ValueError(f"Cannot set reserved property: {key}")

        if property_type is None:
            property_type = self.COMMON_PROPERTIES.get(key, TablePropertyType.STRING)

        # Validate and create property
        prop = TableProperty(key=key, value=value, property_type=property_type)

        # Set in Databricks
        self.spark.sql(f"""
            ALTER TABLE {self.table_name} 
            SET TBLPROPERTIES ('{key}' = '{value}')
        """)

        # Update local metadata
        if self._metadata:
            self._metadata.properties[key] = prop

    def unset_property(self, key: str) -> None:
        """Unset a table property."""
        if key in self.RESERVED_PROPERTIES:
            raise ValueError(f"Cannot unset reserved property: {key}")

        self.spark.sql(f"""
            ALTER TABLE {self.table_name} 
            UNSET TBLPROPERTIES IF EXISTS ('{key}')
        """)

        if self._metadata and key in self._metadata.properties:
            del self._metadata.properties[key]

    def _get_constraints(self) -> dict[str, Any]:
        """Get table constraints."""
        df = self.spark.sql(f"""
            SELECT column_name, constraint_type, constraint_name
            FROM system.information_schema.key_column_usage AS cu
            JOIN system.information_schema.table_constraints AS tc
                USING (constraint_catalog, constraint_schema, constraint_name)
            WHERE cu.table_name = '{self.metadata.table.lower()}'
                AND cu.table_catalog = '{self.metadata.catalog}'
                AND cu.table_schema = '{self.metadata.schema}'
        """)
        return df.toPandas().to_dict()

    def _get_tags(self) -> list[str]:
        """Get table tags."""
        df = self.spark.sql(f"""
            SELECT tag_name
            FROM system.information_schema.table_tags
            WHERE table_name = '{self.metadata.table.lower()}'
                AND catalog_name = '{self.metadata.catalog}'
                AND schema_name = '{self.metadata.schema}'
        """)
        return [row.tag_name for row in df.collect()]

    def _get_ingestion_pattern(self) -> str | None:
        """Get ingestion pattern from table properties."""
        properties = self._get_table_properties()
        if self.INGESTION_PATTERN_KEY in properties:
            value = properties[self.INGESTION_PATTERN_KEY].value
            return str(value) if value is not None else None
        return None

    def set_ingestion_pattern(self, pattern: str) -> None:
        """Set the ingestion pattern for the table.

        Args:
            pattern: The ingestion pattern (e.g. 'CDC', 'TRUNCATE_LOAD')
        """
        self.set_property(
            self.INGESTION_PATTERN_KEY, pattern.upper(), TablePropertyType.STRING
        )

    def unset_ingestion_pattern(self) -> None:
        """Remove the ingestion pattern property."""
        self.unset_property(self.INGESTION_PATTERN_KEY)

    def _get_location(self) -> str:
        """Get table location."""
        details = self._delta_table.detail()
        if details is None:
            return ""
        row = details.select("location").first()
        return row.location if row else ""

    def _get_last_update(self) -> datetime | None:
        """Get last update timestamp."""
        df = self._delta_table.history(1)
        row = df.first()
        return row.timestamp if row else None

    def _get_last_operation(self) -> str | None:
        """Get last operation type."""
        df = self._delta_table.history(1)
        row = df.first()
        return row.operation if row else None

    def _get_operation_metrics(self) -> dict[str, Any] | None:
        """Get last operation metrics."""
        df = self._delta_table.history(1)
        row = df.first()
        return row.operationMetrics if row else None
