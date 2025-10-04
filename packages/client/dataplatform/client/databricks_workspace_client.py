from typing import Optional

from databricks.sdk import WorkspaceClient
from dataplatform.core.logger import get_logger

logger = get_logger()


def get_environment() -> str:
    """
    Detect the current Databricks environment based on workspace URL.

    Returns:
        str: Environment name ('dev', 'prod', or 'unknown')
    """
    w = WorkspaceClient()
    workspace_id = w.get_workspace_id()

    if workspace_id == 4013475860062973:
        logger.info("environment: dev")
        return "dev"
    elif workspace_id == 3308223633991411:
        logger.info("environment: prod")
        return "prod"
    else:
        logger.warning(f"Warning: Unknown workspace ID: {workspace_id}")
        return "unknown"


def get_catalog_for_environment(
    catalog_type: str, environment: Optional[str] = None
) -> str:
    """Get the appropriate catalog name for the current environment.

    Args:
        catalog_type: The type of catalog ("silver", "gold", etc.)
        environment: Optional explicit environment ('dev', 'prod', 'unknown').
                    If None, automatically detects the environment.

    Returns:
        The actual catalog name for the current environment
    """
    # Use provided environment or auto-detect
    env = environment if environment is not None else get_environment()

    catalog_mapping = {
        "dev": {
            "feature_store": "dev_feature_store",
            "bronze": "dev_bronze",
            "silver": "dev_silver",
            "gold": "dev_gold",
        },
    }

    env_mapping = catalog_mapping.get(env, {})
    result = env_mapping.get(catalog_type, catalog_type)

    return result


def get_dbutils():
    """
    Get the dbutils object.
    This can only run inside a databricks cluster
    """
    w = WorkspaceClient()
    return w.dbutils


def set_jobs_task_values(key: str, value: str):
    """
    Set the values for a job task.
    This can only run inside a databricks cluster

    Example:
        set_jobs_task_values(
            "model_name", "member_retention_probability_model"
        )
        set_jobs_task_values("model_version", "1")


    Args:
        key: The key of the task
        value: The value of the task
    """
    dbutils = get_dbutils()
    dbutils.jobs.taskValues.set(key, value)


def get_jobs_task_values(key: str) -> str:
    """
    Get the value for a job task.
    This can only run inside a databricks cluster
    """
    dbutils = get_dbutils()
    return dbutils.jobs.taskValues.get(key)
