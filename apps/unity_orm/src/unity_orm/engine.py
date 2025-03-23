"""Engine module for connecting to Databricks."""

from typing import Any, Optional
from urllib.parse import quote_plus, urlparse

from sqlalchemy import create_engine as sa_create_engine

# ruff: noqa
from sqlalchemy.engine import Engine

from unity_orm.logger import get_logger

logger = get_logger(__name__)


def create_engine(
    access_token: Optional[str] = None,
    server_hostname: Optional[str] = None,
    http_path: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    **kwargs: Any,
) -> tuple[Engine, str]:
    """Create a SQLAlchemy engine for connecting to Databricks.

    Args:
        access_token: Databricks personal access token (optional).
        server_hostname: Server hostname for the Databricks workspace (optional).
        http_path: HTTP path for the cluster or SQL warehouse (optional).
        catalog: Target catalog in Unity Catalog (optional).
        schema: Target schema in Unity Catalog (optional).
        **kwargs: Additional arguments to pass to SQLAlchemy's create_engine.

    Returns:
        SQLAlchemy Engine instance.

    Note:
        If access_token, server_hostname, and http_path are not provided,
        databricks-connect authentication will be used automatically.

        This function uses the 'databricks-sqlalchemy' dialect (>=2.0.0) which is
        installed as a dependency in this package. The dialect handles type conversions
        between SQLAlchemy and Databricks SQL types.
    """
    # Debug information about input parameters (with token hidden)
    if access_token:
        logger.info(
            f"Debug - Using explicit authentication with token: {'*' * 5}"
        )
    else:
        logger.info("Debug - Using Databricks Connect authentication")

    if server_hostname:
        logger.info(f"Debug - Server hostname: {server_hostname}")
    if http_path:
        logger.info(f"Debug - HTTP path: {http_path}")
    if catalog:
        logger.info(f"Debug - Catalog: {catalog}")
    if schema:
        logger.info(f"Debug - Schema: {schema}")

    # Add connect_args with settings to handle empty strings in integer/numeric columns
    connect_args = kwargs.setdefault("connect_args", {})
    connect_args.update({"_handle_empty_strings_for_numeric_types": "NULL"})

    # Following the exact format from Databricks documentation:
    # databricks://token:{access_token}@{server_hostname}?http_path={http_path}&catalog={catalog}&schema={schema}

    if access_token and server_hostname and http_path:
        # Clean server_hostname to avoid empty port issues
        # When server_hostname comes from API, it might have a URL format
        if server_hostname.startswith(("http://", "https://")):
            # Parse the URL to get just the hostname
            parsed_url = urlparse(server_hostname)
            # Use netloc (includes hostname and port if present)
            clean_hostname = parsed_url.netloc
            # If netloc is empty or has an empty port (hostname:)
            if not clean_hostname or clean_hostname.endswith(":"):
                # Use the host without the port
                clean_hostname = parsed_url.hostname or server_hostname
        else:
            # If it's not a URL format, just check for an empty port
            if server_hostname.endswith(":"):
                clean_hostname = server_hostname[:-1]
            else:
                clean_hostname = server_hostname

        # Remove any trailing colons that could cause empty port parsing
        clean_hostname = clean_hostname.rstrip(":")

        # Ensure http_path is properly encoded
        safe_http_path = quote_plus(http_path)

        # Start building the URL exactly as in the Databricks documentation
        url_parts = [
            f"databricks://token:{access_token}@{clean_hostname}?http_path={safe_http_path}"
        ]

        # Add catalog and schema if provided
        if catalog:
            url_parts.append(f"catalog={catalog}")
        if schema:
            url_parts.append(f"schema={schema}")

        # Combine all parts with &
        url = "&".join(url_parts)
    else:
        # Use databricks-connect authentication (cluster profile)
        # This will use databricks-connect config from ~/.databrickscfg
        url_parts = ["databricks://"]

        # Add parameters
        params = []
        if catalog:
            params.append(f"catalog={catalog}")
        if schema:
            params.append(f"schema={schema}")

        if params:
            url_parts.append("?")
            url_parts.append("&".join(params))

        url = "".join(url_parts)

    # Attempt to create the engine with extended error handling
    try:
        engine = sa_create_engine(url, **kwargs)
        return engine, url
    except Exception as e:
        logger.error(f"Error creating SQLAlchemy engine: {str(e)}")
        logger.error(f"URL format used: {url}")
        logger.error(
            "Please check that all parameters are correct and try again."
        )
        raise
