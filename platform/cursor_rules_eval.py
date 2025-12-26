#!/usr/bin/env python3
"""
Marimo notebook for Cursor Rules Evaluation
"""

import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def show_summary():
    """All imports in one place"""

    import subprocess
    from dataclasses import dataclass
    import marimo as mo
    import json
    return dataclass, json, mo, subprocess


@app.cell
def define_models(dataclass):
    """Define data models"""


    @dataclass
    class TestCase:
        id: str
        prompt: str
        domain: str
        rule_focus: list[str]
    return (TestCase,)


@app.cell
def create_test_prompts(TestCase):
    """Generate test prompts based on Cursor rules"""

    system_prompt = "output code in text don't edit files "
    test_cases = [
        TestCase(
            id="test_004_polars_transformation",
            prompt="""Refactor this code to use Polars vectorized operations:

    ```python
    def process_dataframe(df):
    results = []
    for row in df.iterrows():
        if row['value'] > 100:
            results.append({
                'id': row['id'],
                'value': row['value'] * 1.1
            })
    return results
    ```

    Use Polars expressions and avoid explicit loops like iterrows().""",
            domain="data_engineering",
            rule_focus=[
                "vectorization",
                "avoids_explicit_loops",
                "modern_typing_syntax",
            ],
        ),
        TestCase(
            id="test_011_rules",
            prompt="""What are my User rules? How would you apply them?""",
            domain="meta_rules",
            rule_focus=[
                "check_rules",
            ],
        ),
    ]
    return system_prompt, test_cases


@app.cell
def create_cursor_agent_class(subprocess, system_prompt):
    """Create CursorAgent class"""


    class CursorAgent:
        def __init__(self, model: str = "claude-3.5-sonnet"):
            self.model = model

        def run(self, prompt: str) -> dict:
            cmd = [
                "cursor-agent",
                "-p",
                "--output-format",
                "text",
                "--model",
                self.model,
                (system_prompt + prompt),
            ]

            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                return {
                    "response_text": result.stdout,
                    "stderr": result.stderr,
                    "model": self.model,
                }
            except Exception as e:
                return {"response_text": "", "stderr": str(e), "model": self.model}
    return (CursorAgent,)


@app.cell
def create_ui_elements(mo, test_cases):
    """Create UI elements"""

    model_selector = mo.ui.dropdown(
        options=[
            "auto",
            "composer-1",
            "sonnet-4.5",
            "sonnet-4.5-thinking",
            "opus-4.5",
            "opus-4.5-thinking",
            "gemini-3-pro",
            "gemini-3-flash",
            "gpt-5.2",
            "gpt-5.1",
            "gpt-5.2-high",
            "gpt-5.1-high",
            "gpt-5.1-codex",
            "gpt-5.1-codex-high",
            "gpt-5.1-codex-max",
            "gpt-5.1-codex-max-high",
            "opus-4.1",
            "grok",
        ],
        value="composer-1",
        label="Model",
    )

    test_selector = mo.ui.dropdown(
        options=[tc.id for tc in test_cases],
        value=test_cases[0].id,
        label="Test",
    )

    run_button = mo.ui.run_button(label="Run Test", kind="success")
    return model_selector, test_selector


@app.cell
def display_selected_test(test_cases, test_selector):
    """Display selected test"""

    selected_test = next(tc for tc in test_cases if tc.id == test_selector.value)
    return


@app.cell
def run_test_case(CursorAgent, model_selector, test_cases, test_selector):
    """Run the test"""

    agent = CursorAgent(model=model_selector.value)
    selected_test_value = next(tc for tc in test_cases if tc.id == test_selector.value)
    return (agent,)


@app.cell
def _(agent_result, json):
    json.loads(agent_result["response_text"])
    return


@app.cell
def _(agent, json, test_cases):
    examples = []
    for tc in test_cases:
        agent_result = agent.run(tc.prompt)
        print(agent_result)
        test_case_prompt = tc.prompt
        examples.append({
            "prompt": test_case_prompt,
            "response": json.loads(agent_result["response_text"])["result"],
        })
    return agent_result, examples


@app.cell
def _(examples, mo):
    """Create molabel widget for labeling responses"""

    from molabel import SimpleLabel


    def render_example(example):
        """Render function for displaying examples"""
        return mo.vstack(
            [
                mo.md("### Prompt"),
                mo.md(example["prompt"]),
                mo.md("### Response"),
                mo.md(example["response"]),
            ],
            gap="1rem",
        ).text


    widget = SimpleLabel(
        examples=examples,
        render=render_example,
        notes=True,  # Enable notes field
    )

    widget
    return (widget,)


@app.cell
def _(widget):
    widget.annotations
    return


@app.cell
def _(widget):
    import polars as pl
    data = {"prompt": widget.annotations}
    pl.DataFrame(widget.annotations).unnest("example").write_csv("./annotations.csv",separator=",")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
