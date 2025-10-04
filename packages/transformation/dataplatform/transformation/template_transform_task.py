from dataplatform.client.managed_table_client import ManagedTableClient
from dataplatform.core.logger import get_logger
from dataplatform.transformation.cleanser import (
    cleanse,
    load_data,
    order_columns,
)
from dataplatform.transformation.transform_validator import validate

logger = get_logger(__name__)

if __name__ == "__main__":
    config = load_data("./config/member.yml")
    cleansed_config = cleanse(config)

    example_df = cleansed_config["example_table"]["dataframe"]

    # Add your transformations here

    column_order = []

    example_df = order_columns(example_df, column_order)

    validate(example_df, config)

    client = ManagedTableClient()
    client.overwrite(example_df, "silver.example.example")
