from collections.abc import Generator
from pathlib import Path
from typing import Any

from databricks.ml_features.entities.feature_function import FeatureFunction
from databricks.ml_features.entities.feature_lookup import FeatureLookup
from databricks.ml_features.online_store_spec.amazon_dynamodb_online_store_spec import (
    timedelta,
)
from fastcore.utils import (  # type: ignore [reportUnusedImport]
    patch,
)
from ruamel.yaml import YAML


def create_feature_lookup(
    table_name: str,
    lookup_key: str,
    feature_names: list[str],
    timestamp_lookup_key: str | None = None,
    rename_outputs: dict[str, str] | None = None,
    lookback_window: timedelta | None = None,
    default_values: dict[str, Any] | None = None,
) -> FeatureLookup:
    """
    Create a FeatureLookup object.

    Args:
        feature_store_name: The name of the feature store.
        lookup_key: The key to use for the lookup.
        feature_names: The names of the features to lookup.
        timestamp_lookup_key: The key to use for the timestamp lookup.
        rename_outputs: The names to use for the outputs.
        lookback_window: The lookback window to use for the lookup.
        default_values: The default values to use for the lookup.

    Returns:
        A FeatureLookup object.
    Example:
        ```python
        create_feature_lookup(
            feature_store_name="feature_store.member.member_features",
            lookup_key="member_id",
            feature_names=["Age", "age_group"],
            timestamp_lookup_key="created_at",
            rename_outputs={"age_group": "demographic_age_group"},
            lookback_window=timedelta(days=30),
            default_values={"age": 18},
        )
        ```
    """
    return FeatureLookup(
        table_name=table_name,
        lookup_key=lookup_key,
        feature_names=feature_names,
        rename_outputs=rename_outputs,
        timestamp_lookup_key=timestamp_lookup_key,
        lookback_window=lookback_window,
        default_values=default_values,
    )


def get_feature_lookups(
    config: dict[str, Any],
) -> Generator[FeatureLookup | FeatureFunction, Any, Any]:
    """
    Get feature lookups from a configuration dictionary.
    Args:
        config: the configuration dictionary of feature lookups.
    Returns:
        A generator of FeatureLookup objects.
    Example:
    ```python
    config = {
        "feature_lookups": [
            {
                "feature_store_name": "feature_store.member.member_features",
                "lookup_key": "member_id",
                "feature_names": ["Age", "age_group"],
            }
            {
                "feature_store_name": "feature_store.merchandising.product_features",
                "lookup_key": "product_id",
                "feature_names": ["product_name", "product_category", "total_sales],
            }
        ]
    }
    for lookup in get_feature_lookups(config):
        print(lookup)
    ```
    """
    yield from (create_feature_lookup(**lookup) for lookup in config["feature_lookups"])


def parse_feature_lookup_config(
    path: str,
) -> Generator[FeatureLookup | FeatureFunction, Any, Any]:
    """
    Parses a feature lookup config file and returns a generator of FeatureLookup.
    The config path can be relative this gets resolved thanks to fastcore.utils.relpath.
    The config is parsed using the ruamel.yaml library.
    Args:
        path: The path to the feature lookup config file.
    Returns:
        A generator of FeatureLookup objects.
    Example:
        ```python
        for lookup in parse_feature_lookup_config("../../feature_lookup.yml"):
            print(lookup)
        ```
    """
    path = Path(path).resolve()
    config = path.read_yaml()
    yield from get_feature_lookups(config)


@patch
def read_yaml(self: Path, type: str = "safe") -> dict[str, Any]:
    """
    Patches the pathlib.Path class with the method read_yaml.
    This method reads a yaml file and returns a dictionary.
    Args:
        self: The path to the yaml file.
        type: The type of yaml to read.
    Returns:
        A dictionary of the yaml file.
    """
    yaml = YAML(typ=type)
    return yaml.load(self)
