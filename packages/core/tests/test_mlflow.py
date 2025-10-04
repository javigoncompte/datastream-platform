import os

import pytest
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from dataplatform.core.setup_mlflow import mlflow_experiment

_ = load_dotenv()


@pytest.fixture
def ws():
    return WorkspaceClient(profile=os.getenv("DATABRICKS_CONFIG_PROFILE"))


@pytest.fixture
def experiment_name():
    return "test_basic"


@pytest.fixture
def experiment_ws():
    return ws


@pytest.fixture
def set_experiment(experiment_name):
    @mlflow_experiment(experiment_name, autolog_modules=["langchain", "lightgbm"])
    def test_function():
        return "test_result"

    yield test_function
    test_function.mlflow_client.delete_experiment(
        test_function.experiment.experiment_id
    )


def test_basic_decorator_functionality(set_experiment):
    """Test basic decorator functionality without autolog modules"""

    assert hasattr(set_experiment, "mlflow_client")
    assert hasattr(set_experiment, "experiment")
    assert hasattr(set_experiment, "experiment_name")
    assert hasattr(set_experiment, "autolog_modules")

    expected_exp_name = "/Shared/Experiments/test_basic"
    print(set_experiment.experiment_name)
    assert set_experiment.experiment_name == expected_exp_name
    assert set_experiment.autolog_modules == ["langchain", "lightgbm"]
