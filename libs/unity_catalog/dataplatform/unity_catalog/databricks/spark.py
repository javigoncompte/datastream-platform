from databricks.connect import DatabricksSession
from databricks.sdk.config import Config
from fastcore.basics import store_attr
from pyspark.sql import SparkSession


class Spark:
    def __init__(self, config: dict[str, str] | None = None, serverless: bool = False):
        store_attr("config,serverless", self)

    @property
    def spark_session(self) -> SparkSession:
        spark = self._create_databricks_session()
        return spark

    def _add_serverless(
        self, builder: DatabricksSession.Builder
    ) -> DatabricksSession.Builder:
        return builder.serverless(True)

    def _add_config(
        self, builder: DatabricksSession.Builder
    ) -> DatabricksSession.Builder:
        return builder.sdkConfig(Config(**self.config))

    def _create_databricks_session(self) -> SparkSession:
        builder = DatabricksSession.builder
        if self.serverless:
            builder = self._add_serverless(builder)
        if self.config is not None:
            builder = self._add_config(builder)
        return builder.getOrCreate()
