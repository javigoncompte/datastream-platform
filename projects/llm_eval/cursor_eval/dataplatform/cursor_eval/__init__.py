"""Cursor Rules Evaluation Package.

This package provides tools for evaluating Cursor agent responses
against user-defined rules using MLflow 3.x scorers and Feedback objects.

Example usage:
    from dataplatform.cursor_eval import evaluate

    result = evaluate(model="sonnet-4")
"""

from dataplatform.cursor_eval.agent import (
    AgentResponse,
    create_predict_fn,
    invoke_cursor_agent,
)
from dataplatform.cursor_eval.eval_runner import (
    build_eval_dataset,
    create_traced_predict_fn,
    evaluate,
)
from dataplatform.cursor_eval.scorers import (
    asked_confirmation,
    get_all_scorers,
    get_scorer_for_rule,
    no_emojis,
    provided_references,
    uses_type_hints,
)
from dataplatform.cursor_eval.test_cases import (
    RuleType,
    TestCase,
    get_all_test_cases,
    get_test_case_by_rule,
)

__all__ = [
    # Agent
    "AgentResponse",
    "invoke_cursor_agent",
    "create_predict_fn",
    # Evaluation
    "evaluate",
    "build_eval_dataset",
    "create_traced_predict_fn",
    # Scorers (MLflow make_judge and @scorer)
    "asked_confirmation",
    "provided_references",
    "uses_type_hints",
    "no_emojis",
    "get_all_scorers",
    "get_scorer_for_rule",
    # Test Cases
    "RuleType",
    "TestCase",
    "get_all_test_cases",
    "get_test_case_by_rule",
]
