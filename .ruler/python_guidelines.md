# Python Project Guidelines

## General Style

- Using `ruff` as a formatter and linter
- Use `ruff` to format and lint your code using pyproject.toml from root
- Mlflow >= 3.0 for Machine Learning/Data Science/GenAI/LLM development and exploration
- Strictly use the `typing` module. All functions, methods, and class members must have type annotations.
- pytest for testing
- Dependency management using UV
- Prefer `async` and `await`
- Strive for elegant and Pythonic code that is easy to understand and maintain.
- Adhere to PEP 8 guidelines for code style, with Ruff as the primary linter and formatter.
- Favor explicit code that clearly communicates its intent over implicit, overly concise code.
- Keep the Zen of Python in mind when making design decisions.

## Code Example Requirements

- All functions must include type annotations.
- Must provide clear, Google-style docstrings no longer than 79 lines.
- Key logic should be annotated with comments.
- Provide usage examples (e.g., in the `tests/` directory or as a `__main__` section).
- Include error handling.


## Modular Design

- Each module/file should have a well-defined, single responsibility.
- Develop reusable functions and classes, favoring composition over inheritance.
- Organize code into logical packages and modules.

## Code Quality

- All functions, methods, and class members must have type annotations, using the most specific types possible.
- All functions, methods, and classes must have Google-style docstrings, thoroughly explaining their purpose, parameters, return values, and any exceptions raised. Include usage examples where helpful.
- Aim for high data validation coverage on Gold table
- Use specific exception types, provide informative error messages, and handle exceptions gracefully. Implement custom exception classes when needed. Avoid bare `except` clauses.
- Employ dataplatform.core.logger at the file level scope. Use: logging = get_logger(__name__). The logger used is [logger.py](mdc:packages/core/logger.py). Logging should be used minimally, ideally at the top and bottom of a function, to log important events, warnings, and errors.

## Reading Data
- When reading data use the managed table client
- Separate reading and writing data, functions should pass data frames never spark 


## Key Principles

- Use functional programming where appropriate; avoid unnecessary classes
- Write concise, technical responses with accurate Python examples.
- Prioritize clarity, efficiency, and best practices in deep learning workflows.
- Use object-oriented programming for model architectures and functional programming for data processing pipelines.
- Implement proper GPU utilization and mixed precision training when applicable.
- Use descriptive variable names that reflect the components they represent.
- Use type hints for all function signatures
- Use descriptive variable names that reflect their purpose
- Structure code modularly with clear separation of concerns

## Data Platform Best Practices

- Use polars/spark/daft/pandas_on_spark for data manipulation and analysis
- Prefer vectorized operations over explicit loops
- Use efficient data structures (e.g., categorical types for strings)
- Implement proper error handling and data validation
- Create reusable data processing pipelines
- Implement proper logging and monitoring

## LLM  Applications

- Data Flyjwheels for LLM applications: <https://www.sh-reya.com/blog/ai-engineering-flywheel/>
- Identify metrics that matter for your specific use case.
- Criteria Drift will happen that is okay.
- Agents: <https://huyenchip.com//2025/01/07/agents.html>
- LLM fundamentals: <https://applied-llms.org/>

## LLM Evaluations

- Use binary yes or no questions for LLM judges since scores are subjective
- Create test cases with dimensions for synthetic test data generation.
- <https://eugeneyan.com/writing/evals/>
- <https://eugeneyan.com/writing/aligneval/>
- <https://eugeneyan.com/writing/llm-evaluators/>
- LLM-as-Judge can work (somewhat), but it’s not a silver bullet
  - Use pairwise comparisons: Instead of asking the LLM to score a single output on a Likert scale, present it with two options and ask it to select the better one. This tends to lead to more stable results.
  - Control for position bias: The order of options presented can bias the LLM’s decision. To mitigate this, do each pairwise comparison twice, swapping the order of pairs each time. Just be sure to attribute wins to the right option after swapping!
  - Allow for ties: In some cases, both options may be equally good. Thus, allow the LLM to declare a tie so it doesn’t have to arbitrarily pick a winner.
  - Control for response length: LLMs tend to bias toward longer responses. To mitigate this, ensure response pairs are similar in length.
  - Use Chain-of-Thought: Asking the LLM to explain its decision before giving a final answer can increase eval reliability. As a bonus, this lets you to use a weaker but faster LLM and still achieve similar results. Because this part of the pipeline is typically run in batch, the extra latency from CoT isn’t a problem.

## Error Handling

- Use try-except blocks for error-prone operations
- Implement proper logging
  - we have a decorator in client
- Use custom exceptions for specific error cases
- Implement proper input validation
- Handle API rate limits and timeouts
- Use tiger style if possible to handle errors and make code efficient

Refer to official documentation:

- Databricks: <https://docs.databricks.com/en/index.html>
- Pyspark: <https://spark.apache.org/docs/latest/api/python/index.html>

## Others

- **Prioritize new features in Python 3.12+.**
- **When explaining code, provide clear logical explanations and code comments.**
- **When making suggestions, explain the rationale and potential trade-offs.**
- **If code examples span multiple files, clearly indicate the file name.**
- **Do not over-engineer solutions. Strive for simplicity and maintainability while still being efficient.**
- **Favor modularity, but avoid over-modularization.**
- **Use the most modern and efficient libraries when appropriate, but justify their use and ensure they don't add unnecessary complexity.**
- **When providing solutions or examples, ensure they are self-contained and executable without requiring extensive modifications.**
- **If a request is unclear or lacks sufficient information, ask clarifying questions before proceeding.**
- **Always consider the security implications of your code, especially when dealing with user inputs and external data.**
- **Actively use and promote best practices for the specific tasks at hand (LLM app development, data cleaning, demo creation, etc.).**
