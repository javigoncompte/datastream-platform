oqa: B018, F841, F821

import marimo

__generated_with = "0.13.15"
app = marimo.App(width="full")

with app.setup:
    # Initialization code that runs before all other cells
    import os

    SCHEMA = "members"
    CATALOG = "qa_agents"
    os.environ["DATABRICKS_HOST"] = "https://***.azuredatabricks.net/"
    os.environ["DATABRICKS_CONFIG_PROFILE"] = "qa-serverless"
    os.environ["DATABRICKS_TOKEN"] = "***"
    os.environ["KEY_VAULT_NAME"] = "***"
    os.environ["DATABRICKS_HTTP_PATH"] = "/sql/1.0/warehouses/***"
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"
    os.environ["DATABRICKS_SERVER_HOSTNAME"] = "***"
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()
    from sqlalchemy import create_engine

    host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")
    catalog = os.getenv("DATABRICKS_CATALOG")
    schema = os.getenv("DATABRICKS_SCHEMA")
    databricks = create_engine(
        f"databricks://token:{access_token}@{host}?http_path={http_path}&catalog={CATALOG}&schema={SCHEMA}"
    )


@app.cell
def _():
    import marimo as mo
    import pandas as pd

    from evalgen.test_cases import (
        TestCase,
        process_test_cases,
    )

    return TestCase, mo, pd, process_test_cases


@app.cell
def _(mo):
    mo.md(
        """
    #EvalGen Workflow
    ##Concept to align Evaluations with Domain Experts. So an alignment of assertions and providing value to our outputs and metrics.
    [Who Validates the Validators? Aligning LLM-Assisted Evaluation of LLM Outputs with Human Preferences](https://arxiv.org/pdf/2404.12272)

        <img src="https://kagi.com/proxy/evalgen-fig1.jpg?c=mfH32_6twc_XDXySFVkmjuz7pXF-YEU3XEvFuQA3k66azBR1cRwBXJn87UeLhlP5YZScMk9PYpOVyU5urCT3Vw%3D%3D" />


        <img src="https://files.catbox.moe/caqcmk.png"/>

    ##Using Eval Driven development for LLM due to the nature of LLM outputs and lack of good validation metrics like in Machine learning.

        <img src="https://pbs.twimg.com/media/GshEu1AbAAAyb95?format=jpg&name=medium"/>
    """
    )


@app.cell
def _(mo):
    mo.md(
        """
    #Criteria Examples:
    ```json
    [
      {
        "criteria": "The itinerary adheres to the required format, including all specified sections in the correct order.",
        "shortname": "format_adherence",
        "eval_method": "expert"
      },
      {
        "criteria": "Activities, accommodations, and additional information are consistent with the trip context and do not conflict with member preferences.",
        "shortname": "context_consistency",
        "eval_method": "expert"
      },
      {
        "criteria": "The response does not include contact information, addresses, or business names, and all special characters are correctly formatted (e.g., '\\u00e9' is converted to '\u00e9').",
        "shortname": "content_validity",
        "eval_method": "code"
      }
    ]
    ```
    #Criteria Results:
    ```json
    [
      [
        "Does the itinerary include all specified sections in the correct order, adhering to the required format?",
        "Does the itinerary comply with the required format, ensuring each section is present, complete, and arranged correctly?",
        "Are all sections of the itinerary included and structured in the correct order as outlined in the criteria?"
      ],
      [
        "Are the activities, accommodations, and additional information in the itinerary consistent with the trip context and free from conflicts with the member preferences?",
        "Does the itinerary ensure that the activities, accommodations, and additional details align with the trip context while avoiding contradictions with the member preferences?",
        "Are the chosen activities, accommodations, and supplementary details relevant to the trip context and do they avoid any conflict with the specified member preferences?"
      ],
      [
        "Does the response exclude contact information, addresses, and business names, and are all special characters correctly formatted (e.g., '\u00e9' instead of '\\u00e9')?",
        "Are there no contact details, addresses, or specific business names in the response, and is the formatting of special characters accurate (e.g., '\\u00e9' converted to '\u00e9')?",
        "Is the response free of contact information, addresses, and business names, and are all special characters properly displayed (e.g., '\u00e9' instead of '\\u00e9')?"
      ]
    ]
    ```
    """
    )


@app.cell
def _(mo):
    button = mo.ui.button(label="Clear")
    button
    return (button,)


@app.cell
def _(button):
    button
    TEST_CASES = []
    return (TEST_CASES,)


@app.cell(hide_code=True)
def _(TEST_CASES, TestCase):
    def create_test_cases(x):
        feature = x["feature"]
        persona = x["persona"]
        constraints = x["constraints"]
        assumptions = x["assumptions"]
        scenario = x["scenario"]
        constraints_list = [
            item.strip() for item in constraints.split(",") if item.strip()
        ]
        assumptions_list = [
            item.strip() for item in assumptions.split(",") if item.strip()
        ]
        test_case = TestCase(
            feature=feature,
            scenario=scenario,
            constraints=constraints_list,
            assumptions=assumptions_list,
            persona=persona,
        )
        return TEST_CASES.append(test_case)

    return (create_test_cases,)


@app.cell
def _(create_test_cases, mo):
    form = (
        mo.md("""
        **Your form.**

        {feature}
        {scenario}
        {constraints}
        {persona}
        {assumptions}
    """)
        .batch(
            feature=mo.ui.text(label="feature"),
            scenario=mo.ui.text(label="scenario"),
            constraints=mo.ui.text(label="constraints"),
            persona=mo.ui.text(label="persona"),
            assumptions=mo.ui.text(label="assumptions"),
        )
        .form(
            show_clear_button=True,
            bordered=False,
            on_change=lambda value: create_test_cases(value),
        )
    )
    return (form,)


@app.cell
def _(form):
    form


@app.cell(hide_code=True)
def _(TEST_CASES, process_test_cases):
    if len(TEST_CASES) > 0:
        test_cases, run_id = process_test_cases(
            TEST_CASES,
            experiment_name="/experiments/evalgen_annotation",
            endpoint_name="open-ai-gpt-4o-endpoint",
        )
    else:
        test_cases = []
    return (test_cases,)


@app.cell
def _(mo, pd, test_cases):
    if len(test_cases) > 0:
        table = mo.ui.data_editor(
            [i.model_dump() for i in test_cases],
        )
    else:
        default_data = pd.DataFrame({
            "feature": [],
            "scenario": [],
            "contraints": [],
            "persona": [],
            "assumptions": [],
            "test_case_prompt": [],
        })
        table = mo.ui.data_editor(default_data)
    return (table,)


@app.cell(hide_code=True)
def _(table):
    table


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
