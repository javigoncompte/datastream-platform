from databricks.connect import DatabricksSession

from unity_orm.logger import get_logger

logger = get_logger(__name__)


def get_spark(profile="qa"):
    """Get a Databricks Spark session.

    Handles session recreation if the session fails due to inactivity.

    Args:
        profile: The Databricks profile to use

    Returns:
        A Databricks Spark session
    """
    try:
        spark = (
            DatabricksSession.builder.serverless(True)
            .profile(profile)
            .getOrCreate()
        )
        return spark
    except Exception as e:
        logger.error(f"Spark session failed: {str(e)}")
        logger.info("Creating new session...")
        # Simply try once more
        return (
            DatabricksSession.builder.serverless(True)
            .profile(profile)
            .getOrCreate()
        )
