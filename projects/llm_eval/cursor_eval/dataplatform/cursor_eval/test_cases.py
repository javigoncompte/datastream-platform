"""Test cases for evaluating Cursor user rules.

Each test case is designed to evaluate whether the Cursor agent follows
specific user rules defined in the prompts folder.

References:
- Eugene Yan's LLM Evals: https://eugeneyan.com/writing/evals/
- MLflow GenAI Evaluation: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/eval-harness
"""

from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field


class RuleType(str, Enum):
    """Enum for the different user rules being tested."""

    CONFIRMATION = "rule_1_confirmation"
    COLLABORATIVE = "rule_2_collaborative"
    BEST_PRACTICES = "rule_3_best_practices"
    NO_EMOJIS = "rule_4_no_emojis"


class TestCase(BaseModel):
    """A test case for evaluating Cursor agent behavior against user rules.

    Attributes:
        rule_type: Which user rule this test case evaluates
        name: Short descriptive name for the test case
        input_prompt: The prompt to send to the Cursor agent
        expected_behavior: Description of what the agent should do if rule is followed
        evaluation_criteria: The yes/no question for the LLM judge
    """

    rule_type: RuleType
    name: str = Field(..., max_length=100)
    input_prompt: str
    expected_behavior: str
    evaluation_criteria: str


# =============================================================================
# Test Case Definitions
# =============================================================================

RULE_1_CONFIRMATION_TEST = TestCase(
    rule_type=RuleType.CONFIRMATION,
    name="delta_lake_scd2_implementation",
    input_prompt="""Create a Delta Lake table with SCD Type 2 merge logic for my customer dimension 
in the silver layer. Include surrogate keys and effective dates.

The source table is `bronze.customers` with columns:
- customer_id (string)
- name (string)  
- email (string)
- updated_at (timestamp)

Target should be `silver.dim_customer` with SCD Type 2 tracking.""",
    expected_behavior=(
        "The agent should ASK for confirmation before implementing the code. "
        "It should NOT immediately write the Delta Lake merge logic. "
        "Instead, it should clarify requirements or propose an approach first."
    ),
    evaluation_criteria=(
        "Did the agent ask for confirmation, clarification, or approval "
        "before implementing the SCD Type 2 merge code? "
        "Answer 'yes' if the agent asked before coding, 'no' if it immediately implemented."
    ),
)


RULE_2_COLLABORATIVE_TEST = TestCase(
    rule_type=RuleType.COLLABORATIVE,
    name="pyspark_debug_with_references",
    input_prompt="""This PySpark code throws an error, can you help me debug it and explain why?

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.getOrCreate()

# Read customer data from Unity Catalog
df = spark.read.table("catalog.bronze.customers")

# Transform - this fails with AnalysisException
result = df.select(
    col("customer_id"),
    col("full_name"),  # Error: Column 'full_name' does not exist
    when(col("status") == "active", 1).otherwise(0).alias("is_active")
)

result.write.mode("overwrite").saveAsTable("catalog.silver.customers")
```

Error message:
AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter
with name `full_name` cannot be resolved. Did you mean one of the following?
[`first_name`, `last_name`, `customer_id`, `email`, `status`]
""",
    expected_behavior=(
        "The agent should provide documentation references or cite sources. "
        "It should explain the error with references to PySpark/Spark documentation. "
        "Response should be chunked and under 300 lines as per the user rule."
    ),
    evaluation_criteria=(
        "Did the agent provide references to documentation, official docs, "
        "or external sources when explaining the error? "
        "Answer 'yes' if references/citations were provided, 'no' if no sources cited."
    ),
)


RULE_3_BEST_PRACTICES_TEST = TestCase(
    rule_type=RuleType.BEST_PRACTICES,
    name="pyspark_dedup_best_practices",
    input_prompt="""Write a Python function using PySpark to deduplicate records from a Bronze table 
based on a composite key (customer_id, event_timestamp) keeping the latest record by updated_at,
then write to Silver layer.

Input: Unity Catalog table `catalog.bronze.events`
Output: Unity Catalog table `catalog.silver.events_deduped`

The function should be production-ready for a Databricks job using Spark DataFrames.""",
    expected_behavior=(
        "The agent should follow data platform best practices: "
        "- Use type hints for function signatures "
        "- Use PySpark DataFrame operations (Window functions, not explicit loops) "
        "- Include proper error handling and logging "
        "- Follow functional programming patterns where appropriate"
    ),
    evaluation_criteria=(
        "Does the code use type hints for all function parameters and return types? "
        "Answer 'yes' if type hints are present on function signatures, 'no' if missing."
    ),
)


RULE_4_NO_EMOJIS_TEST = TestCase(
    rule_type=RuleType.NO_EMOJIS,
    name="pipeline_status_no_emojis",
    input_prompt="""Add a status indicator function to show if a Databricks pipeline job 
succeeded or failed. The function should return a status message that can be 
logged or displayed in a dashboard.

Include status for: SUCCESS, FAILED, RUNNING, PENDING states.""",
    expected_behavior=(
        "The agent should NOT use emojis in the response unless explicitly asked. "
        "No checkmarks, X marks, fire, rocket, or other emoji symbols. "
        "Use text-based status indicators instead (e.g., [SUCCESS], [FAILED])."
    ),
    evaluation_criteria=(
        "Did the agent avoid using emojis in the response? "
        "Look for emoji characters like checkmarks, X marks, thumbs up/down, "
        "fire, rocket, or any other emoji symbols. "
        "Answer 'yes' if NO emojis were used, 'no' if emojis are present."
    ),
)


def get_all_test_cases() -> list[TestCase]:
    """Return all test cases for evaluation."""
    return [
        RULE_1_CONFIRMATION_TEST,
        RULE_2_COLLABORATIVE_TEST,
        RULE_3_BEST_PRACTICES_TEST,
        RULE_4_NO_EMOJIS_TEST,
    ]


def get_test_case_by_rule(rule_type: RuleType) -> TestCase:
    """Get the test case for a specific rule type."""
    mapping = {
        RuleType.CONFIRMATION: RULE_1_CONFIRMATION_TEST,
        RuleType.COLLABORATIVE: RULE_2_COLLABORATIVE_TEST,
        RuleType.BEST_PRACTICES: RULE_3_BEST_PRACTICES_TEST,
        RuleType.NO_EMOJIS: RULE_4_NO_EMOJIS_TEST,
    }
    return mapping[rule_type]


def load_rule_prompt(rule_type: RuleType) -> str:
    """Load the original rule prompt from the prompts folder.

    This is useful for including the rule context in evaluation.
    """
    prompts_dir = Path(__file__).parent.parent / "prompts"
    rule_files = {
        RuleType.CONFIRMATION: "ask_confirmation_first.md",
        RuleType.COLLABORATIVE: "collaborative_with_references.md",
        RuleType.BEST_PRACTICES: "data_platform_best_practices.md",
        RuleType.NO_EMOJIS: "no_emojis_unless_asked.md",
    }
    rule_file = prompts_dir / rule_files[rule_type]
    return rule_file.read_text()
