import datetime
import logging
import time
from typing import Any

import mlflow
import pandas as pd
from databricks.agents import datasets, review_app
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import NotFound

from . import CATALOG, SCHEMA

log = logging.getLogger(__name__)


def get_user_name() -> str:
    w = WorkspaceClient()
    user = w.current_user.me()
    if user_name := user.user_name:
        return user_name
    raise ValueError("User name not found")


def get_review_app(experiment_name: str = "/experiments/evalgen"):
    experiment = init_mlflow(experiment_name)
    review = review_app.get_review_app(experiment.experiment_id)
    return review


def init_mlflow(
    experiment_name: str = "/experiments/evalgen",
) -> mlflow.entities.Experiment:
    mlflow.login()
    mflow_client = mlflow.MlflowClient(registry_uri="databricks-uc")
    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    experiment = mlflow.set_experiment(experiment_name)
    return experiment, mflow_client


def create_label_schemas(
    review: review_app.ReviewApp, criterias: list[dict[str, str]]
) -> list[str]:
    label_schemas = []
    for criteria in criterias:
        if criteria["eval_method"] == "expert":
            label_schema = review.create_label_schema(
                type="feedback",
                input=review_app.label_schemas.InputCategorical(options=["yes", "no"]),
                name=criteria["shortname"],
                title=criteria["criteria"],
                overwrite=True,
                enable_comment=True,
            )
            label_schemas.append(label_schema.name)
    return label_schemas


def labeling_sesion(
    agent_name: str,
    requests: list[dict[str, str]],
    criterias: list[dict[str, str]],
    new_session: bool = True,
) -> review_app.LabelingSession:
    init_mlflow()
    user_name = get_user_name()
    table_name = f"{CATALOG}.{SCHEMA}.evalgen_{agent_name}"
    try:
        if new_session:
            datasets.delete_dataset(table_name)
    except NotFound as e:
        raise e
    dataset = datasets.create_dataset(table_name)
    dataset.insert(requests)
    review = get_review_app()
    clear_all_labeling_sessions(review)
    label_schemas_list = create_label_schemas(review, criterias)
    labeling_session = review.create_labeling_session(
        name="evalgen_labeling_session",
        assigned_users=[user_name],
        label_schemas=label_schemas_list,
    )
    log.info(f"Labeling session created: {labeling_session.url}")
    return labeling_session


def clear_all_labeling_sessions(review: review_app.ReviewApp):
    labeling_sessions = review.get_labeling_sessions()
    for labeling_session in labeling_sessions:
        review.delete_labeling_session(labeling_session)


def annotation_session(run_id: str) -> review_app.LabelingSession:
    _, mflow_client = init_mlflow("/experiments/evalgen_annotation")
    review = get_review_app("/experiments/evalgen_annotation")
    clear_all_labeling_sessions(review)
    traces: pd.DataFrame = mlflow.search_traces(run_id=run_id, return_type="pandas")  # type: ignore
    timestamp_filter = int(
        (datetime.datetime.now() - datetime.timedelta(minutes=10)).timestamp() * 1000
    )
    traces_df = traces[traces["timestamp_ms"] >= timestamp_filter]  # type: ignore

    user_name = get_user_name()
    label_schema = review.create_label_schema(
        name="good_response",
        type="feedback",
        enable_comment=True,
        overwrite=True,
        title="good_response",
        input=review_app.label_schemas.InputCategorical(options=["yes", "no"]),
    )

    labeling_session = review.create_labeling_session(
        "evalgen_labeling_annotation",
        assigned_users=[user_name],
        label_schemas=[label_schema.name],
    )
    labeling_session.add_traces(traces_df)
    log.info(f"Annotation session created: {labeling_session.url}")
    print(labeling_session.url)
    return labeling_session


def is_response_good(asssesments: list[Any]) -> bool | None:
    for assessment in asssesments:
        if assessment.name == "good_response":
            return (
                assessment.feedback.value == "yes" or assessment.feedback.value == "no"
            )
    return None


def sync_annotation_session(
    labeling_session: review_app.LabelingSession,
    agent_name: str,
    poll_interval: int = 30,
) -> None:
    """Sync annotation session with polling for user input.

    Args:
        labeling_session: The labeling session to sync
        agent_name: Name of the agent
        poll_interval: Time between polls in seconds
    """
    last_trace_count = 0

    while True:
        traces = mlflow.search_traces(
            run_id=labeling_session.mlflow_run_id, return_type="pandas"
        )

        *_, current_trace_count = traces.shape

        if current_trace_count > last_trace_count:
            log.info(f"Found {current_trace_count - last_trace_count} new traces")
            last_trace_count = current_trace_count

        response_values = (
            traces["assessments"].apply(is_response_good).value_counts(dropna=False)
        )

        log.info(f"Response values: {response_values}")

        if not response_values.isna().all():
            log.info("Found assessments, proceeding with sync")
            break

        log.info(f"Waiting for user input... Checking again in {poll_interval} seconds")
        time.sleep(poll_interval)

    labeling_session.sync_expectations(
        to_dataset=f"qa_agents.default.evalgen_{agent_name}"
    )
    log.info(f"Synced expectations to dataset: qa_agents.default.evalgen_{agent_name}")


def process_annotation_session(agent_name: str, run_id: str):
    my_session = annotation_session(run_id)
    sync_annotation_session(my_session, agent_name)
    return my_session
