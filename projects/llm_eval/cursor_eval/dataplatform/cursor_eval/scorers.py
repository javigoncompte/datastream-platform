"""Scorers for evaluating Cursor agent responses against user rules.

Uses MLflow 3.x make_judge() for LLM-based evaluation and @scorer for code-based tests.

References:
- MLflow Custom Judges: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/custom-judge/create-custom-judge
- MLflow Custom Scorers: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/custom-scorers
- Eugene Yan's LLM Evaluators: https://eugeneyan.com/writing/llm-evaluators/
"""

import logging
from typing import Any, Literal

from mlflow.entities import Feedback
from mlflow.genai.judges import make_judge
from mlflow.genai.scorers import scorer

logger = logging.getLogger(__name__)

JUDGE_MODEL = "databricks:/databricks-gpt-oss-120b"

asked_confirmation = make_judge(
    name="asked_confirmation",
    instructions=(
        "Evaluate if the AI agent asked for confirmation before implementing code.\n\n"
        "User's request:\n{{ inputs }}\n\n"
        "Agent's response:\n{{ outputs }}\n\n"
        "The agent should NOT immediately write implementation code. "
        "It should first propose an approach, ask questions, or seek confirmation.\n\n"
        "Answer 'yes' if the agent asked before coding, 'no' if it immediately implemented."
    ),
    feedback_value_type=Literal["yes", "no"],
    model=JUDGE_MODEL,
)

provided_references = make_judge(
    name="provided_references",
    instructions=(
        "Evaluate if the AI agent cited documentation or external sources.\n\n"
        "User's question:\n{{ inputs }}\n\n"
        "Agent's response:\n{{ outputs }}\n\n"
        "Look for: URLs, mentions of 'documentation', 'official docs', "
        "'according to', or specific version references.\n\n"
        "Answer 'yes' if sources were cited, 'no' if no references were provided."
    ),
    feedback_value_type=Literal["yes", "no"],
    model=JUDGE_MODEL,
)

uses_type_hints = make_judge(
    name="uses_type_hints",
    instructions=(
        "Evaluate if the Python code uses type hints for function signatures.\n\n"
        "User's request:\n{{ inputs }}\n\n"
        "Agent's response:\n{{ outputs }}\n\n"
        "Look for type annotations like `def func(x: int) -> str:` or `param: list[str]`.\n\n"
        "Answer 'yes' if type hints are present on function signatures, "
        "'no' if functions lack type annotations."
    ),
    feedback_value_type=Literal["yes", "no"],
    model=JUDGE_MODEL,
)


@scorer
def no_emojis(
    *,
    outputs: Any | None = None,
) -> Feedback:
    """Code-based scorer for Rule 4: Did the agent avoid using emojis?

    Reference: https://eugeneyan.com/writing/evals/
    Code tests are preferred when criteria can be evaluated programmatically.
    """
    import emoji

    response = str(outputs) if outputs else ""
    emojis_found = emoji.emoji_list(response)
    passed = len(emojis_found) == 0

    if passed:
        rationale = "No emojis detected in the response."
        value = "yes"
    else:
        unique_emojis = list({e["emoji"] for e in emojis_found})[:10]
        rationale = f"Emojis detected: {unique_emojis}"
        value = "no"

    logger.info(f"Scorer 'no_emojis': {value}, count={len(emojis_found)}")

    return Feedback(value=value, rationale=rationale)


def get_all_scorers() -> list:
    """Return all scorers for use with mlflow.genai.evaluate()."""
    return [asked_confirmation, provided_references, uses_type_hints, no_emojis]


def get_scorer_for_rule(rule_name: str):
    """Get the appropriate scorer function for a rule."""
    scorers = {
        "confirmation": asked_confirmation,
        "collaborative": provided_references,
        "best_practices": uses_type_hints,
        "no_emojis": no_emojis,
    }
    return scorers.get(rule_name)
