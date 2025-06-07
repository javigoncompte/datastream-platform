import json
from typing import Any

from langchain.output_parsers.json import SimpleJsonOutputParser
from langchain.prompts import ChatPromptTemplate

from dataplatform.evalgen.databricks_integration.llms import evalgen_wizard_model
from dataplatform.evalgen.evalgen_prompt import (
    build_function_gen_prompt,
    evaluation_criteria,
    system_msg,
)


def evalgen_criteria_processing(
    criterias: list[dict[str, str]],
    prompt_template: str,
    vars_context: dict[str, Any] | None = None,
    bad_example: list[str] | None = None,
):
    llm = evalgen_wizard_model(model="gpt-4o")
    results = []
    for criteria in criterias:
        criteria_description = criteria.get("criteria", "")
        eval_method = criteria.get("eval_method", "")
        prompt = build_function_gen_prompt(
            criteria_description=criteria_description,
            eval_method=eval_method,
            prompt_template=prompt_template,
            bad_example=bad_example,
            vars_context=vars_context,
        )
        chain = llm | SimpleJsonOutputParser()
        output = chain.invoke(prompt)
        results.append(output)
    return results


def evalgen_criteria_generation(
    prompt: str,
    user_feedback_prompt: str,
    output_file: str,
):
    llm = evalgen_wizard_model(model="gpt-4o")
    prompt_template = ChatPromptTemplate.from_messages([
        ("system", system_msg),
        ("user", evaluation_criteria),
    ])
    chain = prompt_template | llm
    criterias = chain.invoke({
        "prompt": prompt,
        "userFeedbackPrompt": user_feedback_prompt,
    })
    with open(f"./src/libraries/evalgen_src/docs/{output_file}", "w") as f:
        json.dump(criterias.content, f, indent=2)
