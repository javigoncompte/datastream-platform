from pathlib import Path

from databricks.bundles.core import (
    Bundle,
    Resources,
    load_resources_from_current_package_module,
)

wheel = next(Path("dist").glob("*.whl"))


def load_resources(bundle: Bundle) -> Resources:
    """
    'load_resources' function is referenced in databricks.yml and is responsible for loading
    bundle resources defined in Python code. This function is called by Databricks CLI during
    bundle deployment. After deployment, this function is not used.
    """

    namespace = "dataplatform"
    variables = bundle.variables
    wheel_name = variables["deployment_name"]
    type_of_package = variables["type_of_deployment"]

    resources_path = f"{type_of_package}/{wheel_name}/{namespace}"
    load_resources.deployment_name = wheel_name
    load_resources.package_path = resources_path
    return load_resources_from_current_package_module()
