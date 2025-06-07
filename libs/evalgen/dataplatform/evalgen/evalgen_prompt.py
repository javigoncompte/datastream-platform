from typing import Any

from langchain.prompts import ChatPromptTemplate
from langchain.schema.runnable import RunnableSerializable
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage

system_msg = """
You are an expert Python programmer and helping me write assertions for my LLM pipeline. An LLM pipeline accepts an example and prompt template, fills the template's placeholders with the example, and generates a response.
"""

evaluation_criteria = """
    Here is my LLM prompt template:

    {prompt}

    {userFeedbackPrompt}

    Based on the instructions in the prompt that need to be followed, I want to write a list of assertions for my LLM pipeline to run on all pipeline responses.
    Give me a list of 3 distinct criteria to check for in LLM responses.
    Each item in the list should contain a string description of a criteria to check for, and whether it should be evaluated with code or by an expert if the criteria is difficult to evaluate.
    Your answer should be a JSON list of objects within json markers, where each object has the following three fields: "criteria", "shortname", and "eval_method" (code or expert).
    At most 3 criteria should have eval_method as expert.
    The "criteria" should be short, and the "shortname" should be a very brief title for the criteria. Each evaluation criteria should test a concept that should evaluate to "true" in the ideal case.

"""


evaluation_criteria_from_description = """
    I've described a criteria I want to use to evaluate text. I want you to take the criteria and output a JSON object in the format below.

    CRITERIA:
    {desc}

    Your response should contain a short title for the criteria ("shortname"), a description of the criteria in 2 sentences ("criteria"), and whether it should be evaluated with "code", or by an "expert" if the criteria is difficult to evaluate ("eval_method").
    Your answer should be JSON within a ```json ``` marker, with the following three fields: "criteria", "shortname", and "eval_method" (code or expert).
    The "criteria" should expand upon the user's input, the "shortname" should be a very brief title for the criteria, and this list should contain as many evaluation criteria as you can think of.
    Each evaluation criteria should test a unit concept that should evaluate to "true" in the ideal case. Only output JSON, nothing else.
    """


def execute_llm_eval(
    llm: BaseChatModel,
    candidate_criteria_prompt: str,
    positive_example: str | None = None,
    negative_example: str | None = None,
) -> RunnableSerializable[dict[str, str], BaseMessage]:
    """Builds the evaluation prompt similar to executeLLMEval in ChainForge.

    Args:
        criteria_text: The criteria text to evaluate against

    Returns:
        The formatted evaluation prompt matching TypeScript implementation
    """
    eval_prompt = (
        "Evaluate the text below according to this criteria: "
        + candidate_criteria_prompt
        + ' Only return "yes" or "no", nothing else.\n\n```\n'
        + "{input}"
        + "\n```"
    )
    if positive_example and negative_example:
        system_msg = (
            "\n\nYou are an expert evaluator. Please consider the following GOOD example:\n"
            + positive_example
            + "\n\nand BAD example:\n"
            + negative_example
        )
    else:
        system_msg = "\n\nYou are an expert evaluator."

    prompt_template = ChatPromptTemplate.from_messages([
        ("system", system_msg),
        ("user", eval_prompt),
    ])
    chain = prompt_template | llm
    return chain


def build_context_prompt_for_vars_metavars(
    vars_and_metavars: dict[str, Any],
) -> str:
    """Builds a context prompt explaining available variables and their values.

    Args:
        vars_and_metavars: Dictionary containing variables and their values

    Returns:
        Formatted string explaining available variables
    """
    context_parts = []

    if vars_and_metavars.get("vars"):
        context_parts.append("Variables available in the prompt:")
        for var_name, var_value in vars_and_metavars["vars"].items():
            context_parts.append(f"- {var_name}: {var_value}")

    if vars_and_metavars.get("metavars"):
        context_parts.append("\nMetadata available:")
        for meta_name, meta_value in vars_and_metavars["metavars"].items():
            context_parts.append(f"- {meta_name}: {meta_value}")

    return "\n".join(context_parts)


def build_function_gen_prompt(
    criteria_description: str,
    eval_method: str,
    prompt_template: str,
    bad_example: list[str] | None = None,
    vars_context: dict[str, Any] | None = None,
) -> str:
    """Builds the function generation prompt similar to buildFunctionGenPrompt in ChainForge.

    Args:
        criteria_shortname: Brief title for the criteria
        criteria_description: Full description of the criteria
        eval_method: "expert" or "code"
        prompt_template: Original LLM pipeline prompt template
        bad_example: Optional example response that doesn't meet criteria
        vars_context: Optional context about template variables

    Returns:
        The formatted prompt for generating evaluation functions
    """
    bad_example_section = ""
    if bad_example:
        bad_example_section = f"""
        Here is an example response that DOES NOT meet the criteria:
        ```
        {bad_example}
        ```
        """

    if eval_method == "expert":
        vars_context_prompt = build_context_prompt_for_vars_metavars(vars_context or {})
        if vars_context_prompt:
            vars_context_prompt = (
                """
            In your prompts, it may be useful to refer to metadata associated with the LLM output, such as when you are comparing to a ground truth.
            For instance, consider a situation where the user has a prompt template with a variable {writing_style} —'poem', 'text message', or 'formal letter' —and they want to validate that the LLM's output was really in that style.
            You would produce a prompt template like: "Respond with 'yes' if the text below is in the style of a {writing_style}}, 'no' if not.
            Only reply with the classification, nothing else." The template indicates that the same {writing_style}} variable used upstream in the LLM pipeline, should be used in your evaluation prompt.
            If you want to refer to the value of an input variable, you **must** use template braces like {variable}. Here are the variables you have access to (keys), and example values for one output:
            """
                + vars_context_prompt
            )

        return f"""
        Given the following prompt template for an LLM pipeline:

        {prompt_template}

        Your task is to devise a prompt for an expert to evaluate the pipeline's responses based on the following criteria: "{criteria_description}"
        {bad_example_section}
        You will devise 3 prompts for the evaluation criterion to see which has the best accuracy. Each prompt you generate should be a short question that an expert can answer with a "yes" or "no" to evaluate entire criteria (don't miss anything in the criteria). Try different variations/wordings in the prompts.
        {vars_context_prompt}

        Return your prompts in a JSON list of strings within markers. Each string should be a question for the expert to answer, and each question should be contained on its own line.

        ---"""

    else:
        return build_eval_code_generation_prompt(
            spec_prompt=criteria_description,
            context=build_context_prompt_for_vars_metavars(vars_context or {}),
            many_funcs=True,
            only_boolean_funcs=False,
        )


def build_eval_code_generation_prompt(
    context: str,
    spec_prompt: str,
    many_funcs: bool = False,
    only_boolean_funcs: bool = False,
) -> str:
    """Builds a prompt for generating evaluation code.

    Args:
        many_funcs: Whether to generate multiple functions
        only_boolean_funcs: Whether functions can only return booleans
        context: Additional context for the prompt
        spec_prompt: User's specification for the evaluation

    Returns:
        Formatted prompt string
    """
    func_plural = "s" if many_funcs else ""
    each_your = "Each" if many_funcs else "Your"

    # Return type restrictions
    return_types = "boolean" if only_boolean_funcs else "boolean, numeric, or string"

    return f"""
    You are to generate {many_funcs and "many different functions" or "one function"} to evaluate textual data, given a user-specified specification.
    The function{func_plural} will be mapped over an array of objects of type ResponseInfo.
    {each_your} solution must contain a single function called 'evaluate' that takes a single object, 'r', of type ResponseInfo. A ResponseInfo is defined as:

    ```python
        text: str  # The text of the LLM response
        prompt: str  # The text of the prompt using to query the LLM
        llm: str  # The name of the LLM queried (the nickname in ChainForge)
        var: dict  # A dictionary of arguments that filled in the prompt template used to generate the final prompt
        meta: dict  # A dictionary of metadata ('metavars') that is 'carried alongside' data used to generate the prompt
    ```

    For instance, here is an evaluator that returns the length of a response:

    ```python
    def evaluate(response):
    # Return the length of the response (num of characters)
    return len(response.text);
    ```

    You can only write in Python.
    You can use imports if necessary. Do not include any type hints.
    Your function{func_plural} can ONLY return {return_types} values.
    {context}
    Here is the user's specification:

    {spec_prompt}"""
