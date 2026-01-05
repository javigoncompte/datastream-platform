from functools import wraps
from typing import List, Optional

import mlflow

from dataplatform.core.logger import get_logger

logger = get_logger(__name__)


def setup_mlflow_experiment(
    experiment_name: str,
    run_name: Optional[str] = None,
    autolog_modules: Optional[List[str]] = None,
):
    """Decorator factory that sets up MLflow experiment for a function

    Args:
        experiment_name: Name of the MLflow experiment. This will be prefixed
                        with /Shared/Experiments/
        run_name: Optional custom name for the MLflow run. If None, MLflow
                  generates a random name.
        autolog_modules: List of MLflow modules to enable autologging for.
                        Examples: ['pytorch', 'langchain', 'xgboost',
                                   'sklearn', 'transformers']

    Examples:
        # Basic usage with manual logging
        @mlflow_experiment("manual_experiment")
        def train_basic_model():
            mlflow.log_param("learning_rate", 0.01)
            mlflow.log_metric("accuracy", 0.95)
            return model

        # Pre-configured decorators for reuse
        pytorch_exp = mlflow_experiment(
            "pytorch_training", autolog_modules=["pytorch"]
        )
        langchain_exp = mlflow_experiment(
            "llm_experiments", autolog_modules=["langchain"]
        )

        @pytorch_exp
        def train_resnet():
            # PyTorch autolog enabled for this experiment
            pass

        @pytorch_exp
        def train_transformer():
            # Same experiment, same autolog settings
            pass

        @langchain_exp
        def qa_chain():
            # LangChain autolog enabled for this experiment
            pass

        @langchain_exp
        def summarization_chain():
            # Same LangChain experiment
            pass

        # PyTorch training with automatic logging
        @mlflow_experiment("pytorch_training", autolog_modules=["pytorch"])
        def train_pytorch_model(data, model):
            # PyTorch metrics, parameters, and model automatically logged
            optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
            # Training loop...
            return trained_model

        # LLM pipeline with LangChain
        @mlflow_experiment("llm_experiment", autolog_modules=["langchain"])
        def run_llm_pipeline(prompt, model_name):
            # LangChain chains, prompts, and responses automatically logged
            chain = LLMChain(llm=OpenAI(), prompt=prompt)
            return chain.run(input_text="Hello world")

        # ML pipeline with multiple frameworks
        @mlflow_experiment(
            "ensemble_training", autolog_modules=["xgboost", "sklearn"]
        )
        def train_ensemble(X_train, y_train):
            # Both XGBoost and sklearn models automatically logged
            xgb_model = xgb.XGBClassifier().fit(X_train, y_train)
            rf_model = RandomForestClassifier().fit(X_train, y_train)
            return {"xgb": xgb_model, "rf": rf_model}


    Returns:
        Decorator function that wraps the target function with MLflow tracking
    """
    # MLflow setup happens once when decorator is created

    mlflow.login()
    experiment_name = f"/Shared/Experiments/{experiment_name}"
    registry_uri = "databricks-uc"
    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri(registry_uri)
    client = mlflow.MlflowClient(registry_uri=registry_uri)
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        experiment_id = client.create_experiment(experiment_name)
        experiment = client.get_experiment(experiment_id)

    # Enable autologging for specified modules
    if autolog_modules:
        try:
            mlflow.autolog(disable=True, silent=True)
        except AttributeError as e:
            raise e

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with mlflow.start_run(
                experiment_id=experiment.experiment_id,
                run_name=run_name,
            ):
                return func(*args, **kwargs)

        # Add attributes to the wrapper function
        wrapper.mlflow_client = client  # type: ignore[attr-defined]
        wrapper.experiment = experiment  # type: ignore[attr-defined]
        wrapper.experiment_name = experiment.name  # type: ignore[attr-defined]
        wrapper.experiment_id = experiment.experiment_id  # type: ignore[attr-defined]
        wrapper.run_name = run_name  # type: ignore[attr-defined]
        wrapper.autolog_modules = autolog_modules or []  # type: ignore[attr-defined]
        return wrapper

    return decorator
