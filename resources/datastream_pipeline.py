from databricks.bundles.pipelines import Pipeline

package_path = "packages/datastream"
notebook_path = "packages/datastream/dlt_pipeline.ipynb"
datastream_pipeline = Pipeline.from_dict({
    "name": "datastream_pipeline",
    "target": "datastream_${bundle.target}",
    ## Specify the 'catalog' field to configure this pipeline to make use of Unity Catalog:
    "catalog": "catalog_name",
    "libraries": [
        {
            "notebook": {
                "path": notebook_path,
            },
        },
    ],
    "configuration": {
        "bundle.sourcePath": "${workspace.file_path}/${package_path}",
    },
})
