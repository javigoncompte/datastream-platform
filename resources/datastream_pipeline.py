from databricks.bundles.pipelines import Pipeline

from . import load_resources

package_path = load_resources.package_path
notebook_path = f"{package_path}/dlt_pipeline.ipynb"
datastream_pipeline = Pipeline.from_dict({
    "name": "datastream_pipeline",
    "serverless": True,
    "target": "datastream_${bundle.target}",
    # Specify the 'catalog' field to configure this pipeline to make use of Unity Catalog:
    "catalog": "test",
    "libraries": [
        {
            "notebook": {
                "path": notebook_path,
            },
        },
    ],
    "configuration": {
        "bundle.sourcePath": "${workspace.file_path}",
    },
})
