"""Tests for evalgen.evalgen_prompt module."""

import pytest
from unittest.mock import patch, MagicMock
from dataplatform.evalgen.evalgen_prompt import (
    execute_llm_eval,
    build_context_prompt_for_vars_metavars,
    build_function_gen_prompt,
    build_eval_code_generation_prompt,
    system_msg,
    evaluation_criteria,
    evaluation_criteria_from_description,
)


class TestExecuteLLMEval:
    """Test cases for execute_llm_eval function."""

    @patch('dataplatform.evalgen.evalgen_prompt.ChatPromptTemplate')
    def test_execute_llm_eval_with_examples(self, mock_chat_template):
        """Test execute_llm_eval with positive and negative examples."""
        # Setup mocks
        mock_llm = MagicMock()
        mock_template = MagicMock()
        mock_chat_template.from_messages.return_value = mock_template
        mock_chain = MagicMock()
        mock_template.__or__ = MagicMock(return_value=mock_chain)
        
        # Execute function
        result = execute_llm_eval(
            llm=mock_llm,
            candidate_criteria_prompt="Test criteria",
            positive_example="Good example",
            negative_example="Bad example"
        )
        
        # Assertions
        assert result == mock_chain
        mock_chat_template.from_messages.assert_called_once()
        # Check that the system message includes examples
        call_args = mock_chat_template.from_messages.call_args[0][0]
        system_message = call_args[0][1]
        assert "Good example" in system_message
        assert "Bad example" in system_message

    @patch('dataplatform.evalgen.evalgen_prompt.ChatPromptTemplate')
    def test_execute_llm_eval_without_examples(self, mock_chat_template):
        """Test execute_llm_eval without examples."""
        mock_llm = MagicMock()
        mock_template = MagicMock()
        mock_chat_template.from_messages.return_value = mock_template
        mock_chain = MagicMock()
        mock_template.__or__ = MagicMock(return_value=mock_chain)
        
        # Execute function without examples
        result = execute_llm_eval(
            llm=mock_llm,
            candidate_criteria_prompt="Test criteria"
        )
        
        # Assertions
        assert result == mock_chain
        mock_chat_template.from_messages.assert_called_once()
        # Check that the system message is simpler without examples
        call_args = mock_chat_template.from_messages.call_args[0][0]
        system_message = call_args[0][1]
        assert system_message == "\n\nYou are an expert evaluator."


class TestBuildContextPromptForVarsMetavars:
    """Test cases for build_context_prompt_for_vars_metavars function."""

    def test_build_context_with_vars_only(self):
        """Test building context with variables only."""
        vars_metavars = {
            "vars": {
                "user_name": "John",
                "age": 25
            }
        }
        
        result = build_context_prompt_for_vars_metavars(vars_metavars)
        
        assert "Variables available in the prompt:" in result
        assert "user_name: John" in result
        assert "age: 25" in result
        assert "Metadata available:" not in result

    def test_build_context_with_metavars_only(self):
        """Test building context with metadata only."""
        vars_metavars = {
            "metavars": {
                "source": "database",
                "timestamp": "2023-01-01"
            }
        }
        
        result = build_context_prompt_for_vars_metavars(vars_metavars)
        
        assert "Metadata available:" in result
        assert "source: database" in result
        assert "timestamp: 2023-01-01" in result
        assert "Variables available in the prompt:" not in result

    def test_build_context_with_both_vars_and_metavars(self):
        """Test building context with both variables and metadata."""
        vars_metavars = {
            "vars": {"name": "Alice"},
            "metavars": {"type": "user"}
        }
        
        result = build_context_prompt_for_vars_metavars(vars_metavars)
        
        assert "Variables available in the prompt:" in result
        assert "name: Alice" in result
        assert "Metadata available:" in result
        assert "type: user" in result

    def test_build_context_with_empty_dict(self):
        """Test building context with empty dictionary."""
        result = build_context_prompt_for_vars_metavars({})
        assert result == ""

    def test_build_context_with_none_values(self):
        """Test building context with None values."""
        vars_metavars = {
            "vars": None,
            "metavars": None
        }
        
        result = build_context_prompt_for_vars_metavars(vars_metavars)
        assert result == ""


class TestBuildFunctionGenPrompt:
    """Test cases for build_function_gen_prompt function."""

    @patch('dataplatform.evalgen.evalgen_prompt.build_context_prompt_for_vars_metavars')
    @patch('dataplatform.evalgen.evalgen_prompt.build_eval_code_generation_prompt')
    def test_build_function_gen_prompt_expert_method(self, mock_build_code_prompt, mock_build_context):
        """Test build_function_gen_prompt with expert evaluation method."""
        mock_build_context.return_value = "Context prompt"
        
        result = build_function_gen_prompt(
            criteria_description="Test criteria",
            eval_method="expert",
            prompt_template="Test prompt template",
            bad_example=["Bad example"],
            vars_context={"test": "context"}
        )
        
        assert "Test criteria" in result
        assert "Test prompt template" in result
        assert "Bad example" in result
        assert "expert" in result
        assert "JSON list of strings" in result
        mock_build_context.assert_called_once_with({"test": "context"})
        mock_build_code_prompt.assert_not_called()

    @patch('dataplatform.evalgen.evalgen_prompt.build_context_prompt_for_vars_metavars')
    @patch('dataplatform.evalgen.evalgen_prompt.build_eval_code_generation_prompt')
    def test_build_function_gen_prompt_code_method(self, mock_build_code_prompt, mock_build_context):
        """Test build_function_gen_prompt with code evaluation method."""
        mock_build_context.return_value = "Context prompt"
        mock_build_code_prompt.return_value = "Code generation prompt"
        
        result = build_function_gen_prompt(
            criteria_description="Test criteria",
            eval_method="code",
            prompt_template="Test prompt template"
        )
        
        assert result == "Code generation prompt"
        mock_build_code_prompt.assert_called_once_with(
            spec_prompt="Test criteria",
            context="Context prompt",
            many_funcs=True,
            only_boolean_funcs=False
        )

    @patch('dataplatform.evalgen.evalgen_prompt.build_context_prompt_for_vars_metavars')
    def test_build_function_gen_prompt_expert_without_bad_example(self, mock_build_context):
        """Test build_function_gen_prompt expert method without bad example."""
        mock_build_context.return_value = "Context prompt"
        
        result = build_function_gen_prompt(
            criteria_description="Test criteria",
            eval_method="expert",
            prompt_template="Test prompt template"
        )
        
        assert "Test criteria" in result
        assert "Test prompt template" in result
        assert "Here is an example response that DOES NOT meet the criteria:" not in result

    @patch('dataplatform.evalgen.evalgen_prompt.build_context_prompt_for_vars_metavars')
    def test_build_function_gen_prompt_expert_with_vars_context(self, mock_build_context):
        """Test build_function_gen_prompt expert method with variables context."""
        mock_build_context.return_value = "Variables: user_name, age"
        
        result = build_function_gen_prompt(
            criteria_description="Test criteria",
            eval_method="expert",
            prompt_template="Test prompt with {user_name}",
            vars_context={"vars": {"user_name": "Alice"}}
        )
        
        assert "Variables: user_name, age" in result
        assert "template braces like {variable}" in result


class TestBuildEvalCodeGenerationPrompt:
    """Test cases for build_eval_code_generation_prompt function."""

    def test_build_eval_code_generation_prompt_single_function(self):
        """Test building code generation prompt for single function."""
        result = build_eval_code_generation_prompt(
            context="Test context",
            spec_prompt="Test specification",
            many_funcs=False,
            only_boolean_funcs=True
        )
        
        assert "one function" in result
        assert "Your solution must contain" in result
        assert "boolean values" in result
        assert "Test context" in result
        assert "Test specification" in result

    def test_build_eval_code_generation_prompt_many_functions(self):
        """Test building code generation prompt for multiple functions."""
        result = build_eval_code_generation_prompt(
            context="Test context",
            spec_prompt="Test specification",
            many_funcs=True,
            only_boolean_funcs=False
        )
        
        assert "many different functions" in result
        assert "Each solution must contain" in result
        assert "boolean, numeric, or string values" in result
        assert "Test context" in result
        assert "Test specification" in result

    def test_build_eval_code_generation_prompt_empty_context(self):
        """Test building code generation prompt with empty context."""
        result = build_eval_code_generation_prompt(
            context="",
            spec_prompt="Test specification",
            many_funcs=True,
            only_boolean_funcs=True
        )
        
        assert "many different functions" in result
        assert "boolean values" in result
        assert "Test specification" in result

    def test_build_eval_code_generation_prompt_includes_example(self):
        """Test that the code generation prompt includes the example."""
        result = build_eval_code_generation_prompt(
            context="",
            spec_prompt="Count characters",
            many_funcs=False,
            only_boolean_funcs=False
        )
        
        assert "def evaluate(response):" in result
        assert "return len(response.text)" in result
        assert "ResponseInfo" in result
        assert "text: str" in result
        assert "prompt: str" in result
        assert "llm: str" in result
        assert "var: dict" in result
        assert "meta: dict" in result


class TestConstants:
    """Test cases for module constants."""

    def test_system_msg_exists(self):
        """Test that system_msg constant exists and is not empty."""
        assert system_msg is not None
        assert len(system_msg) > 0
        assert "expert Python programmer" in system_msg

    def test_evaluation_criteria_exists(self):
        """Test that evaluation_criteria constant exists and is not empty."""
        assert evaluation_criteria is not None
        assert len(evaluation_criteria) > 0
        assert "{prompt}" in evaluation_criteria
        assert "{userFeedbackPrompt}" in evaluation_criteria
        assert "JSON list" in evaluation_criteria

    def test_evaluation_criteria_from_description_exists(self):
        """Test that evaluation_criteria_from_description constant exists."""
        assert evaluation_criteria_from_description is not None
        assert len(evaluation_criteria_from_description) > 0
        assert "{desc}" in evaluation_criteria_from_description
        assert "criteria" in evaluation_criteria_from_description
        assert "shortname" in evaluation_criteria_from_description
        assert "eval_method" in evaluation_criteria_from_description