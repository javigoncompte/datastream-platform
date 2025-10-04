from pathlib import Path
from typing import NamedTuple

from pyspark.sql import DataFrame
from dataplatform.client.feature_store_service.feature_lookup import parse_feature_lookup_config
from dataplatform.client.feature_store_service.training_set import create_training_set


class TrainingSetColumns(NamedTuple):
    label_column: str
    exclude_columns: list[str]


def get_training_set_columns(yaml_path: str):
    path = Path(yaml_path).resolve()
    config = path.read_yaml()

    return TrainingSetColumns(
        label_column=config.get("label_column", None),
        exclude_columns=config.get("exclude_columns", []),
    )


def run(
    yaml_path: str,
    label_df: DataFrame,
):
    feature_lookups = list(parse_feature_lookup_config(yaml_path))

    label_column, exclude_columns = get_training_set_columns(yaml_path)
    training_set = create_training_set(
        label_df=label_df,
        label_column=label_column,
        feature_lookups=feature_lookups,
        exclude_columns=exclude_columns,
    )
    return training_set
