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
    if access_token:
        logger.info(f"Debug - Using explicit authentication with token: {'*' * 5}")
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

    connect_args = kwargs.setdefault("connect_args", {})
    connect_args.update({"_handle_empty_strings_for_numeric_types": "NULL"})

    if access_token and server_hostname and http_path:
        if server_hostname.startswith(("http://", "https://")):
            parsed_url = urlparse(server_hostname)
            clean_hostname = parsed_url.netloc
            if not clean_hostname or clean_hostname.endswith(":"):
                clean_hostname = parsed_url.hostname or server_hostname
        else:
            if server_hostname.endswith(":"):
                clean_hostname = server_hostname[:-1]
            else:
                clean_hostname = server_hostname

        clean_hostname = clean_hostname.rstrip(":")

        safe_http_path = quote_plus(http_path)

        url_parts = [
            f"databricks://token:{access_token}@{clean_hostname}?http_path={safe_http_path}"
        ]

        if catalog:
            url_parts.append(f"catalog={catalog}")
        if schema:
            url_parts.append(f"schema={schema}")

        url = "&".join(url_parts)
    else:
        url_parts = ["databricks://"]

        params = []
        if catalog:
            params.append(f"catalog={catalog}")
        if schema:
            params.append(f"schema={schema}")

        if params:
            url_parts.append("?")
            url_parts.append("&".join(params))

        url = "".join(url_parts)

    try:
        engine = sa_create_engine(url, **kwargs)
        return engine, url
    except Exception as e:
        logger.error(f"Error creating SQLAlchemy engine: {str(e)}")
        logger.error(f"URL format used: {url}")
        logger.error("Please check that all parameters are correct and try again.")
        raise
