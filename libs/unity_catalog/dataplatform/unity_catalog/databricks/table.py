from delta.tables import DeltaTable
from fastcore.basics import store_attr
from pyspark.sql import DataFrame, SparkSession

from .spark import Spark


class Table:
    def __init__(self, name: str):
        store_attr("name", self)
        self.spark: SparkSession = Spark().spark_session

    def __repr__(self):
        return f"Table(name={self.name})"

    @property
    def delta_table(self) -> DeltaTable:
        return DeltaTable.forName(self.spark, self.name)

    @property
    def history(self) -> DataFrame:
        return self.delta_table.history()

    @property
    def details(self) -> DataFrame:
        return self.delta_table.details()

    @property
    def version(self) -> int:
        return self.delta_table.version()

    def generate_manifest(self, mode: str = "symlink_format_manifest") -> None:
        return self.delta_table.generate_manifest(mode)

    def vacuum(self, hours: float | None = None) -> DataFrame:
        return self.delta_table.vacuum(hours)
