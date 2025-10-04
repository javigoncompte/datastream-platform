import os
from typing import Dict, Optional

from delta.tables import DeltaTable
from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    lit,
    max,
    to_timestamp,
)
from pyspark.sql.types import TimestampType
from dataplatform.client.databricks_workspace_client import (
    get_environment,
)
from dataplatform.core.logger import get_logger
from dataplatform.core.spark_session import SparkSession
from dataplatform.core.table_infra import create_table

os.environ["PYDEVD_WARN_SLOW_RESOLVE_TIMEOUT"] = "10"


class ManagedTableClient:
    BACKFILL_TIMESTAMP = "1900-01-01 00:00:00"
    CREATED_COL: str = "created_date"
    MODIFIED_COL: str = "last_modified_date"

    def __init__(
        self, spark: Optional[SparkSession] = None, environment: Optional[str] = None
    ):
        """
        Initialize a new ManagedTableClient instance.

        Sets up the environment and database name based on the current
        Databricks workspace.
        """
        self._environment = environment if environment else get_environment()
        self._spark = (spark or SparkSession()).get_spark_session()
        self._logger = get_logger()

    def read(self, table_name: str, options: Optional[Dict] = None) -> DataFrame:
        options = options if options is not None else {}
        table = self._get_table_name(table_name)
        self._logger.info(f"READING from table {table} with options: {options}")
        return self._spark.read.format("delta").options(**options).table(table)

    def read_starting_version(
        self, table_name: str, starting_version: int
    ) -> DataFrame:
        table = self._get_table_name(table_name)

        self._logger.info(
            f"READING from table {table} starting from version: {starting_version}"
        )

        return (
            self._spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", starting_version)
            .table(table)
        )

    def overwrite(
        self, df: DataFrame, table_name: str, options: Optional[Dict] = None
    ) -> None:
        options = options if options else {"overwriteSchema": "true"}

        table = self._get_table_name(table_name)

        df.write.format("delta").mode("overwrite").options(**options).saveAsTable(table)

        self._logger.info(f"OVERWRITING table: {table}")

    def merge(
        self, source_df: DataFrame, target_table: str, key_columns: list[str]
    ) -> None:
        """
        Upsert with audit columns:
        - Inserts: created_date = now, last_modified_date = now
        - Updates: only when non-key/non-audit columns changed;
                   created_date preserved, last_modified_date = now
        Args:
            source_df: Source DataFrame to upsert from (columns must align
                with target schema).
            target_table: Logical target table name
                (mapped by `_get_table_name`).
            key_columns: Non-null business keys used to match
                target rows (equality join).
        """
        table_name = self._get_table_name(target_table)
        _table = create_table(table_name, source_df, primary_key_cols=key_columns)

        insert_expr, update_expr, is_created_added, is_modified_added = (
            self._prepare_audit_and_upsert(table_name, source_df)
        )

        merge_condition = " AND ".join([
            f"target.{k} = source.{k}" for k in key_columns
        ])
        change_cond = self._change_predicate(
            source_df, key_columns, {self.CREATED_COL, self.MODIFIED_COL}
        )

        (
            DeltaTable.forName(self._spark, table_name)
            .alias("target")
            .merge(source_df.alias("source"), condition=merge_condition)
            .withSchemaEvolution()
            .whenMatchedUpdate(condition=change_cond, set=update_expr)
            .whenNotMatchedInsert(values=insert_expr)
            .execute()
        )

        if is_created_added or is_modified_added:
            self._backfill_null_audit_cols(table_name)

        self._logger.info(f"MERGE into {table_name} completed.")

    def get_max_commit_version(
        self, table_name: str, commit_version_col: str = "bronze_commit_version"
    ) -> int:
        full_table_name = self._get_table_name(table_name)

        row = (
            self._spark.table(full_table_name)
            .agg(max(col(commit_version_col)).alias("max_version"))
            .first()
        )

        max_version = (
            -1 if row is None or row["max_version"] is None else int(row["max_version"])
        )

        self._logger.info(
            f"Max {commit_version_col} in {full_table_name}: {max_version}"
        )
        return max_version

    def get_current_commit_version(self, table_name: str) -> int:
        history_df = DeltaTable.forName(
            self._spark, self._get_table_name(table_name)
        ).history(1)
        return (
            history_df.select("version")
            .orderBy("version", ascending=False)
            .first()["version"]
        )

    def _is_dev_catalog(
        self, catalog: str, schema: str, environment: Optional[str] = None
    ) -> bool:
        """
        Determine if a table should use a dev-prefixed catalog name.

        This encapsulates the logic from get_catalog_for_environment but returns
        a boolean indicating whether the table should use a dev-prefixed catalog.

        Args:
            catalog: The catalog name (e.g., "bronze", "silver", "gold")
            schema: The schema name (e.g., "spatial", "rockerbox", "census")
            environment: Optional explicit environment ('dev', 'prod', 'unknown').
                        If None, automatically detects the environment.

        Returns:
            bool: True if the table should use a dev-prefixed catalog name

        Note:
            - Bronze catalog: Only uses dev_ prefix for specific schemas.
            - Sandbox catalog: Never uses dev_ prefix regardless of environment.
            - Other catalogs: Use dev_ prefix in dev environment.
        """
        env = environment if environment is not None else get_environment()

        if not env or env not in {"dev", "prod", "unknown"}:
            raise ValueError(
                f"Environment must be in ('dev', 'prod', 'unknown') but got {env}."
            )

        # Special bronze catalog logic
        if catalog == "bronze":
            if env == "dev" and schema:
                dev_bronze_schemas = ("spatial", "rockerbox", "census")
                if schema in dev_bronze_schemas:
                    self._logger.info(
                        f"Schema '{schema}' requires dev_bronze in dev environment"
                    )
                    return True
            return False

        # Sandbox catalog should never be prefixed with dev_
        if catalog == "sandbox":
            return False

        # For other catalogs, use dev prefix in dev environment
        if env == "dev":
            return True

        return False

    def _get_table_name(self, table_name: str) -> str:
        """
        Resolve table name based on environment and schema.

        Bronze catalog behavior:
        - Special schemas (spatial, rockerbox, census): use dev_bronze in dev,
                                                        bronze in prod
        - Other schemas: use bronze in all environments

        Sandbox catalog behavior:
        - Never uses dev_ prefix regardless of environment

        Other catalogs maintain existing behavior:
        - In dev environment: use dev_ prefix
        - In prod environment: use original name

        Args:
            table_name: The logical table name (e.g., "bronze.schema.table")

        Returns:
            The resolved table name for the current environment
        """
        catalog, schema, table_name = self._parse_table_name(table_name)

        if self._is_dev_catalog(
            catalog=catalog,
            schema=schema,
            environment=self._environment,
        ):
            resolved_catalog = f"dev_{catalog}"
        else:
            resolved_catalog = catalog

        return f"{resolved_catalog}.{schema}.{table_name}"

    def _parse_table_name(self, table_name: str) -> tuple[str, str, str]:
        """
        Parse a fully qualified table name into catalog and schema components.

        Args:
            table_name: The fully qualified table name (e.g., "bronze.schema.table")

        Returns:
            A tuple of (catalog, schema, table_name)

        Raises:
            ValueError: If table_name format is invalid
        """
        parts = table_name.split(".")
        if len(parts) != 3:
            raise ValueError(
                f"Invalid table name format: '{table_name}'. "
                f"Expected format: 'catalog.schema.table'"
            )

        catalog, schema, table_name = parts

        return catalog.strip(), schema.strip(), table_name.strip()

    def _append_audit_cols(
        self, table_name: str, source_df: DataFrame
    ) -> tuple[bool, bool]:
        """
        Ensure the target Delta table has audit timestamp columns
        and evolve its schema if needed.

        This is a **schema-only** operation. It checks whether the table
        `table_name` exists and whether the audit columns `self.CREATED_COL` and
        `self.MODIFIED_COL` are present.If either column is missing
        (or the table does not yet exist), it writes a **0-row** DataFrame based on
        `source_df.schema` with the missing audit columns appended as`TIMESTAMP` (NULL)
        and `mergeSchema=true`, which evolves/creates the table.

        Existing data is **not** modified by this method; new columns will be NULL until
        your MERGE logic or a separate backfill assigns values.

        Args:
            table_name: Fully qualified Delta table name (e.g., "catalog.schema.table").
            source_df: DataFrame whose schema is used as the base for schema evolution.

        Returns:
            tuple[bool, bool]: `(created_added, modified_added)` flags indicating
            whether `self.CREATED_COL` and `self.MODIFIED_COL` were added
            during this call.
        """
        try:
            existing_cols = {
                f.name for f in self._spark.table(table_name).schema.fields
            }
            table_existed = True
        except AnalysisException:
            existing_cols = frozenset()
            table_existed = False

        is_created_missing = self.CREATED_COL not in existing_cols
        is_modified_missing = self.MODIFIED_COL not in existing_cols

        if not table_existed or is_created_missing or is_modified_missing:
            tmp_df = self._spark.createDataFrame([], schema=source_df.schema)
            if self.CREATED_COL not in tmp_df.columns:
                tmp_df = tmp_df.withColumn(
                    self.CREATED_COL, lit(None).cast(TimestampType())
                )
            if self.MODIFIED_COL not in tmp_df.columns:
                tmp_df = tmp_df.withColumn(
                    self.MODIFIED_COL, lit(None).cast(TimestampType())
                )

            (
                tmp_df.limit(0)
                .write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(table_name)
            )

            self._logger.info(
                f"Evolved schema on {table_name}: "
                f"added {
                    [
                        c
                        for c in [
                            self.CREATED_COL if is_created_missing else None,
                            self.MODIFIED_COL if is_modified_missing else None,
                        ]
                        if c
                    ]
                }"
            )
            return is_created_missing, is_modified_missing

        self._logger.info(f"No schema changes needed for {table_name}.")
        return False, False

    def _change_predicate(
        self, df: DataFrame, keys: list[str], audit_cols: set[str]
    ) -> str:
        """
        Args:
            df: Source DataFrame used to determine comparable columns.
            keys: Key columns to exclude from change detection.
            audit_cols: Audit columns to exclude from change detection.

        Returns:
            A tuple of:
            - SQL string predicate that is true only when any comparable
                column changed (NULL-safe),
        """
        excluded = set(keys) | audit_cols
        cols = [c for c in df.columns if c not in excluded]
        if not cols:
            return "false"
        terms = [f"(target.{c} <=> source.{c})" for c in cols]
        return f"NOT ({' AND '.join(terms)})"

    def _prepare_audit_and_upsert(self, table_name: str, df: DataFrame):
        is_created_added, is_modified_added = self._append_audit_cols(table_name, df)

        business_cols = [
            c for c in df.columns if c not in {self.CREATED_COL, self.MODIFIED_COL}
        ]

        insert_expr = {c: col(f"source.{c}") for c in business_cols}
        update_expr = {c: col(f"source.{c}") for c in business_cols}

        # INSERT: set both to now()
        insert_expr[self.CREATED_COL] = current_timestamp()
        insert_expr[self.MODIFIED_COL] = current_timestamp()

        update_expr[self.MODIFIED_COL] = current_timestamp()

        return insert_expr, update_expr, is_created_added, is_modified_added

    def _backfill_null_audit_cols(self, table_name: str) -> None:
        dt = DeltaTable.forName(self._spark, table_name)
        dt.update(
            condition=(
                col(self.CREATED_COL).isNull() | col(self.MODIFIED_COL).isNull()
            ),
            set={
                self.CREATED_COL: coalesce(
                    col(self.CREATED_COL), to_timestamp(lit(self.BACKFILL_TIMESTAMP))
                ),
                self.MODIFIED_COL: coalesce(
                    col(self.MODIFIED_COL), to_timestamp(lit(self.BACKFILL_TIMESTAMP))
                ),
            },
        )
