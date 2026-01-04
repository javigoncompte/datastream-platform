"""MLflow evaluation runner for Cursor rules testing.

This module orchestrates the evaluation of Cursor agent responses
against user rules using MLflow 3.x GenAI evaluation framework.

References:
- MLflow GenAI Evaluation: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/eval-harness
- MLflow Custom Scorers: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/custom-scorers
"""

import logging
from typing import Any

import mlflow

from dataplatform.core.mlflow_experiment import setup_mlflow_experiment
from dataplatform.cursor_eval.agent import invoke_cursor_agent
from dataplatform.cursor_eval.scorers import get_all_scorers
from dataplatform.cursor_eval.test_cases import get_all_test_cases

logger = logging.getLogger(__name__)


def build_eval_dataset() -> list[dict[str, Any]]:
    """Build evaluation dataset from test cases.

    Returns data in the MLflow recommended format:
    [{"inputs": {"prompt": "..."}}, ...]

    Reference: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/eval-harness
    """
    test_cases = get_all_test_cases()
    return [{"inputs": {"prompt": tc.input_prompt}} for tc in test_cases]


def create_traced_predict_fn(model: str | None = None):
    """Create a traced predict function for MLflow evaluation.

    The predict_fn accepts kwargs from inputs dict and returns a dict.
    Uses @mlflow.trace for automatic tracing.

    Reference: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/eval-harness
    """

    @mlflow.trace(name="cursor_agent_predict")
    def predict_fn(prompt: str) -> dict[str, Any]:
        """Traced predict function that invokes cursor-agent."""
        response = invoke_cursor_agent(prompt=prompt, model=model)

        if not response.success:
            return {
                "response": f"ERROR: {response.error}",
                "success": False,
                "error": response.error,
            }

        return {
            "response": response.content,
            "success": True,
            "model": response.model,
        }

    return predict_fn


@setup_mlflow_experiment(experiment_name="cursor_rules_eval")
def evaluate(
    model: str | None = None,
) -> Any:
    """Run evaluation using MLflow 3.x mlflow.genai.evaluate().

    This is the recommended approach using direct evaluation mode.

    Reference: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/eval-harness

    Args:
        model: Model for cursor-agent (e.g., 'sonnet-4', 'gpt-5')
        experiment_name: MLflow experiment name

    Returns:
        MLflow EvaluationResult object
    """
    mlflow.set_experiment("/Shared/Experiments/cursor_rules_eval")
    eval_data = build_eval_dataset()
    predict_fn = create_traced_predict_fn(model=model)
    scorers = get_all_scorers()

    logger.info(f"Running evaluation with {len(eval_data)} test cases")
    logger.info(
        f"Scorers: {[s.name if hasattr(s, 'name') else str(s) for s in scorers]}"
    )

    result = mlflow.genai.evaluate(
        data=eval_data,
        predict_fn=predict_fn,
        scorers=scorers,
    )

    logger.info("Evaluation complete")
    return result


if __name__ == "__main__":
    result = evaluate(model="opus-4.5")
    print(result)
