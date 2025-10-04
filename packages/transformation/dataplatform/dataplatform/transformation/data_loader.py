from typing import Any, Dict

from dataplatform.client.managed_table_client import ManagedTableClient
from dataplatform.core.logger import get_logger

logger = get_logger(__name__)


def load_tables_from_config(
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Loads a table from the database into a Spark DataFrame.

    Args:
        config: Configuration dictionary containing table definitions. Each
                table definition should include at least "table_name".

    Returns:
        config: The updated configuration dictionary, with a "dataframe" key
                added to each table's definition, containing the loaded
                DataFrame.

    Raises:
        ValueError: If conflicting options (both timestamp and commit version)
                    are provided for CDC, or if ending_timestamp is given without
                    starting_timestamp.
    """
    managed_table_client = ManagedTableClient()

    for values in config.values():
        table_name = values.get("table_name")

        if not table_name:
            raise ValueError(
                "Each table definition in the config must have a 'table_name'."
            )

        df = managed_table_client.read(table_name)
        values["dataframe"] = df

    return config
