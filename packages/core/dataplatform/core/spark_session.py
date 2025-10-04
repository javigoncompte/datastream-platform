import copy
import logging
import os

from databricks.sdk import WorkspaceClient
from pyspark.errors import PySparkAttributeError

log = logging.getLogger(__name__)

print(os.getenv("DATABRICKS_CONFIG_FILE"))
print(os.getenv("DATABRICKS_CONFIG_PROFILE"))


class SparkSession:
    def __init__(self):
        self.databricks_profile = os.getenv("DATABRICKS_CONFIG_PROFILE")
        self.databricks_config_file_path = os.getenv("DATABRICKS_CONFIG_FILE")
        self.spark = self._create_spark_session()

    def _get_workspace_client(self):
        if self.databricks_config_file_path and self.databricks_profile:
            return WorkspaceClient(
                config_file=self.databricks_config_file_path,
                profile=self.databricks_profile,
            )
        else:
            return WorkspaceClient()

    def _create_spark_session(self):
        if not self.databricks_config_file_path and not self.databricks_profile:
            return self._create_pyspark_session()
        try:
            return self._create_databricks_session()
        except ImportError:
            return self._create_pyspark_session()
        except RuntimeError as runtime_error:
            if (
                "Only remote Spark Sessions using Databricks Connect are "
                "supported" in str(runtime_error)
            ):
                return self._create_databricks_session()
            else:
                raise runtime_error

    def _create_databricks_session(self):
        import os
        import sys

        os.environ["PYDEVD_WARN_EVALUATION_TIMEOUT"] = "90"

        import polars as pl
        from databricks.connect import DatabricksSession
        from pyspark.sql.connect.dataframe import DataFrame

        def toPolars(self: DataFrame) -> pl.DataFrame:
            query = self._plan.to_proto(self._session.client)
            table = self._session.client.to_table(query)
            local_table = copy.copy(table)
            return pl.from_arrow(local_table)

        DataFrame.toPolars = toPolars

        sys.setrecursionlimit(100000)
        w = self._get_workspace_client()
        if w.config:
            spark = DatabricksSession.builder.sdkConfig(w.config).getOrCreate()
        else:
            spark = DatabricksSession.builder.remote(serverless=True).getOrCreate()
        return spark

    def _create_pyspark_session(self):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        try:
            spark.sparkContext.setCheckpointDir("/checkpointsdir")
        except PySparkAttributeError as e:
            log.warning(
                "Working on interactive shell in notebook sparkContext api is"
                f" not available: {e}",
            )
        return spark

    def get_spark_session(self):
        return self.spark
