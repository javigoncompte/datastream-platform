"""Wrapper for invoking the Cursor Agent CLI.

This module provides a Pythonic interface to the `cursor-agent` CLI tool
for programmatic evaluation of Cursor's AI responses.

References:
- cursor-agent CLI: `cursor-agent -h` for available options
- MLflow GenAI patterns: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/eval-harness
"""

import json
import logging
import subprocess
from dataclasses import dataclass
from typing import Literal

logger = logging.getLogger(__name__)


@dataclass
class AgentResponse:
    """Response from the Cursor Agent CLI.

    Attributes:
        content: The text response from the agent
        model: The model used for generation
        success: Whether the CLI call succeeded
        error: Error message if call failed
        raw_output: The raw CLI output (for debugging)
    """

    content: str
    model: str | None = None
    success: bool = True
    error: str | None = None
    raw_output: str | None = None


OutputFormat = Literal["text", "json", "stream-json"]


def invoke_cursor_agent(
    prompt: str,
    model: str | None = None,
    output_format: OutputFormat = "text",
    timeout_seconds: int = 120,
) -> AgentResponse:
    """Invoke the cursor-agent CLI with a prompt.

    Uses the `--print` flag for non-interactive mode which gives access
    to all tools including write and bash.

    Args:
        prompt: The prompt to send to the agent
        model: Model to use (e.g., 'gpt-5', 'sonnet-4', 'sonnet-4-thinking')
               If None, uses the default model
        output_format: Output format ('text', 'json', 'stream-json')
        timeout_seconds: Timeout for the CLI call

    Returns:
        AgentResponse with the agent's response content

    Example:
        >>> response = invoke_cursor_agent(
        ...     prompt="Explain Delta Lake merge syntax", model="sonnet-4"
        ... )
        >>> print(response.content)
    """
    cmd = ["cursor-agent", "--print", "--output-format", output_format]

    if model:
        cmd.extend(["--model", model])

    cmd.append(prompt)

    logger.info(f"Invoking cursor-agent with model={model}, format={output_format}")
    logger.debug(f"Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )

        if result.returncode != 0:
            logger.error(f"cursor-agent failed: {result.stderr}")
            return AgentResponse(
                content="",
                model=model,
                success=False,
                error=result.stderr,
                raw_output=result.stdout,
            )

        content = result.stdout.strip()

        if output_format == "json":
            content = _parse_json_response(content)

        return AgentResponse(
            content=content,
            model=model,
            success=True,
            raw_output=result.stdout,
        )

    except subprocess.TimeoutExpired:
        logger.error(f"cursor-agent timed out after {timeout_seconds}s")
        return AgentResponse(
            content="",
            model=model,
            success=False,
            error=f"Timeout after {timeout_seconds} seconds",
        )
    except FileNotFoundError:
        logger.error("cursor-agent CLI not found. Is it installed?")
        return AgentResponse(
            content="",
            model=model,
            success=False,
            error="cursor-agent CLI not found. Run: cursor-agent install-shell-integration",
        )


def _parse_json_response(raw_json: str) -> str:
    """Parse JSON response and extract the content field."""
    try:
        data = json.loads(raw_json)
        if isinstance(data, dict):
            for key in ["content", "response", "message", "text", "output"]:
                if key in data:
                    return str(data[key])
            return json.dumps(data, indent=2)
        return str(data)
    except json.JSONDecodeError:
        return raw_json


def create_predict_fn(model: str | None = None):
    """Create a predict function compatible with MLflow evaluate.

    This returns a function that can be passed to mlflow.genai.evaluate()
    as the predict_fn parameter.

    Args:
        model: The model to use for all predictions

    Returns:
        A callable that takes a dict with 'input_prompt' and returns
        a dict with 'response'

    Example:
        >>> predict_fn = create_predict_fn(model="sonnet-4")
        >>> result = mlflow.genai.evaluate(
        ...     data=test_cases_df, predict_fn=predict_fn, scorers=[my_scorer]
        ... )

    References:
        https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/eval-harness
    """

    def predict(inputs: dict) -> dict:
        """Predict function for MLflow evaluate."""
        prompt = inputs.get("input_prompt", inputs.get("prompt", ""))
        response = invoke_cursor_agent(prompt=prompt, model=model)

        return {
            "response": response.content,
            "model": response.model,
            "success": response.success,
            "error": response.error,
        }

    return predict
