from databricks.bundles.jobs import Job

from . import load_resources

"""
The main job for datastream.
"""

resources_path = str(load_resources.package_path)
print(f"resources_path: {resources_path}")
notebook_path = f"{resources_path}/notebook.ipynb"

datastream_job = Job.from_dict({
    "name": "datastream_job",
    "trigger": {},
    # "email_notifications": {
    #     "on_failure": [
    #         "jcompte@inspirato.io",
    #     ],
    # },
    "tasks": [
        {
            "task_key": "notebook_task",
            "notebook_task": {
                "notebook_path": notebook_path,
            },
            "environment_key": "default",
        },
        {
            "task_key": "main_task",
            "environment_key": "default",
            "depends_on": [
                {
                    "task_key": "notebook_task",
                },
            ],
            "python_wheel_task": {
                "package_name": "datastream",
                "entry_point": "main",
            },
            "libraries": [
                # By default we just include the .whl file generated for the datastream package.
                # See https://docs.databricks.com/dev-tools/bundles/library-dependencies.html
                # for more information on how to add other libraries.
                {
                    "whl": "dist/*.whl",
                },
            ],
        },
    ],
    "environments": [
        {
            "environment_key": "default",
            "spec": {
                "dependencies": ["dist/*.whl"],
                "environment_version": "3",
            },
        }  # pyright: ignore[reportArgumentType]
    ],
})
