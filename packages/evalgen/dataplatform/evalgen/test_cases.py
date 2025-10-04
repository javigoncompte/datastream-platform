import logging
from typing import Any

import mlflow
from databricks_langchain import ChatDatabricks
from langchain_core.prompts import (
    ChatPromptTemplate,
    FewShotChatMessagePromptTemplate,
)
from mlflow.deployments import get_deploy_client
from pydantic import BaseModel

from dataplatform.evalgen.databricks_integration import init_mlflow


class TestCase(BaseModel):
    feature: str
    scenario: str
    persona: str
    constraints: list[str] | None = []
    assumptions: list[str] | None = []
    test_case_prompt: str | None = None
    test_case_output: str | None = None


def get_test_case_output(
    endpoint_name: str,
    prompt: str,
    experiment_name: str,
) -> str:
    log = logging.getLogger("mlflow")
    mlflow.autolog()
    deploy_client = get_deploy_client("databricks")
    input_data = {"messages": [{"role": "user", "content": prompt}]}

    mlflow.log_param("prompt", prompt)
    mlflow.log_param("experiment_name", experiment_name)
    match deploy_client:
        case None:
            raise ValueError(
                "Deploy Client has not been set please check your environment variables"
            )
        case mlflow.deployments.databricks.DatabricksDeploymentClient:
            response: mlflow.deployments.databricks.DatabricksEndpoint = (
                deploy_client.predict(
                    endpoint=endpoint_name,
                    inputs=input_data,
                )
            )
            choices = response["choices"]
            *_, last_choice = choices  # type: ignore
            return str(last_choice["message"]["content"])
        case _:
            log.info(f"Using Deploy Client type: {type(deploy_client)}")
            raise NotImplementedError(
                f"Deploy Client type {type(deploy_client)} is not supported"
            )


def generate_test_case_prompt(
    model: str,
    test_case: TestCase,
    temperature: float = 0.7,
    max_tokens: int = 1000,
    top_p: float = 1.0,
) -> str:
    examples = [
        {
            "input": TestCase(
                feature="Order Tracking",
                scenario="Invalid Data Provided",
                persona="Frustrated Customer",
                assumptions=["Order number #1234 does not exist in the system"],
                constraints=["Be professional and concise"],
            ),
            "output": (
                "Generate a user input from someone who is clearly irritated and "
                "impatient, using short, terse language to demand information about "
                "their order status for order number #1234. Include hints of previous "
                "negative experiences."
            ),
        }
    ]
    example_prompt = ChatPromptTemplate.from_messages([
        ("human", "{input}"),
        ("ai", "{output}"),
    ])
    few_shot_prompt = FewShotChatMessagePromptTemplate(
        examples=examples,
        example_prompt=example_prompt,
    )
    prompt = ChatPromptTemplate.from_messages([
        (
            "system",
            (
                "You are a helpfull assistant. That generates llm prompts to generate "
                "a user input for a given test case. Which has a feature(Specific "
                "functionalities of your A.I product), scenario(Situations or problems "
                "the AI may encounter and needs to handle.), persona(Representative "
                "user profiles with distinct characteristics and needs.), "
                "assumptions(The user's assumptions about the scenario), "
                "constraints(The user's constraints on the scenario)."
            ),
        ),
        few_shot_prompt,
        ("human", "{input}"),
    ])
    llm = ChatDatabricks(
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        top_p=top_p,
    )
    chain = prompt | llm
    return str(chain.invoke({"input": test_case.model_dump_json()}).content)


def generate_test_case(
    test_case: TestCase,
    experiment_name: str,
    endpoint_name: str,
    model_for_test_case_prompt: str = "open-ai-gpt-4o-endpoint",
) -> TestCase:
    test_case_prompt = generate_test_case_prompt(
        model=model_for_test_case_prompt, test_case=test_case
    )
    test_case.test_case_prompt = test_case_prompt
    test_case_output = get_test_case_output(
        endpoint_name, test_case_prompt, experiment_name
    )
    test_case.test_case_output = test_case_output
    return test_case


def process_test_cases(
    test_cases: list[TestCase], experiment_name: str, endpoint_name: str
) -> tuple[list[TestCase], str]:
    experiment = init_mlflow(experiment_name)
    final_test_cases = []
    with mlflow.start_run(experiment_id=experiment.experiment_id) as run:
        mlflow.autolog()
        run_id: str = run.info.run_id
        for test_case in test_cases:
            updated_test_case = generate_test_case(
                test_case, experiment_name, endpoint_name
            )
            final_test_cases.append(updated_test_case)
    return final_test_cases, run_id  # pyright: ignore[reportPossiblyUnboundVariable ]


def get_output_from_agent(
    test_cases: list[TestCase],
    agent_name: str,
    version: str | None = None,
    alias: str | None = None,
) -> tuple[list[str], str]:
    responses: list[str] = []
    experiment = init_mlflow(experiment_name="/experiments/evalgen_annotation")
    client = mlflow.MlflowClient()
    if alias:
        version = client.get_model_version_by_alias(agent_name, alias).version
    model_version_uri = f"models:/{agent_name}/{version}"
    agent = mlflow.pyfunc.load_model(model_version_uri)
    with mlflow.start_run(experiment_id=experiment.experiment_id) as run:
        mlflow.autolog()
        mlflow.set_tag("Agent", agent_name)
        mlflow.set_tag("Version", version)
        run_id = run.info.run_id
        for test_case in test_cases:
            input_data = {
                "messages": [{"role": "user", "content": test_case.test_case_output}],
            }
            response: dict[str, Any] | None = agent.predict(data=input_data)
            if response is None:
                responses.append(
                    f"No response from agent for test case: {test_case.model_dump()}"
                )
                continue
            messages = response.get("messages", None)
            if messages:
                *_, last_message = messages
                responses.append(str(last_message["content"]))
            else:
                responses.append(
                    f"No response from agent for test case: {test_case.model_dump()}"
                )
    return responses, run_id  # pyright: ignore[reportPossiblyUnboundVariable ]
