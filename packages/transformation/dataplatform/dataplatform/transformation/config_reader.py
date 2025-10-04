import importlib.resources
import inspect
import os
from typing import Any, Dict

from ruamel.yaml import YAML
from dataplatform.core.logger import get_logger

logger = get_logger()


def read_config(config_name: str, package: str) -> Dict[str, Any]:
    """
    Load a YAML configuration file by reading it as a package resource.

    Args:
        config_name (str): Name of the YAML config file to load
                           (e.g., 'silver_club_club.yml').
        package (str): Importable package path where the config resource may
                       reside (e.g., 'dataplatform.club_analytics.config').

    Returns:
        dict: Parsed contents of the YAML configuration file.

    Raises:
        FileNotFoundError: If the config file cannot be found as a package
                            resource or in the filesystem.
    """
    yaml = YAML(typ="safe")
    # Try to load as a package resource
    try:
        logger.info(
            f"Trying to load config as package resource: {package}.{config_name}"
        )
        with importlib.resources.open_text(package, config_name) as f:
            config: Dict[str, Any] = yaml.load(f)
            logger.info(
                f"Config loaded successfully from package resource: {config_name}"
            )
            return config

    except (
        FileNotFoundError,
        ModuleNotFoundError,
        ImportError,
    ):
        logger.info(f"Config not found as package resource: {config_name}.")
        # Fallback: Try to load from filesystem path
        try:
            logger.info(f"Trying to load config from filesystem: {config_name}")

            # Get the caller's file path instead of cwd
            caller_frame = inspect.currentframe().f_back
            caller_file_path = caller_frame.f_code.co_filename
            caller_dir = os.path.dirname(os.path.abspath(caller_file_path))
            # Go up one level from transforms directory to find config directory
            parent_dir = os.path.dirname(caller_dir)
            config_path: str = os.path.join(parent_dir, "config", config_name)

            logger.info(f"Config path: {config_path}")
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.load(f)
                logger.info(
                    f"Config loaded successfully from filesystem: {config_name}"
                )
                return config
        except FileNotFoundError:
            logger.error(f"Config file not found in filesystem: {config_name}.")
    except Exception as e:
        logger.warning(f"Unexpected error loading config as package resource: {e}.")
        raise e
