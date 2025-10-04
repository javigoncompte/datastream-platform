from typing import List

from databricks.feature_engineering.client import FeatureEngineeringClient
from databricks.ml_features.entities.feature_function import FeatureFunction
from databricks.ml_features.entities.feature_lookup import FeatureLookup
from pyspark.sql import DataFrame


def create_training_set(
    label_df: DataFrame,
    label_column: str,
    feature_lookups: List[FeatureLookup | FeatureFunction] | None = None,
    exclude_columns: list[str] | None = None,
) -> DataFrame:
    """
    Create a training set from a label DataFrame and a list of feature lookups.
    Args:
        label_df: The label DataFrame.
        label_column: The column to use as the label.
        feature_lookups: The feature lookups to use.
        exclude_columns: The columns to exclude from the training set. ex: id.
    Returns:
        A DataFrame containing the training set.
    """
    if exclude_columns is None:
        exclude_columns = []
    fe = FeatureEngineeringClient()
    training_set = fe.create_training_set(
        df=label_df,
        label=label_column,
        feature_lookups=feature_lookups,
        exclude_columns=exclude_columns,
    )
    return training_set.load_df()
