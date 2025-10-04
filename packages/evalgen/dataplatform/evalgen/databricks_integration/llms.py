from typing import Any

from databricks_langchain import ChatDatabricks


def evalgen_wizard_model(
    model: str,
    temperature: float = 0.7,
    max_tokens: int = 1000,
    top_p: float = 1.0,
    **kwargs: Any,
):
    return ChatDatabricks(
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        top_p=top_p,
        **kwargs,
    )
