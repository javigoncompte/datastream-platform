"""Tests for evalgen.main module."""

import json
import pytest
from unittest.mock import patch, mock_open, MagicMock
from dataplatform.evalgen.main import (
    evalgen_criteria_processing,
    evalgen_criteria_generation,
)


class TestEvalgenCriteriaProcessing:
    """Test cases for evalgen_criteria_processing function."""

    def setup_method(self):
        """Set up test data."""
        self.sample_criterias = [
            {
                "criteria": "Response should be factually accurate",
                "eval_method": "expert"
            },
            {
                "criteria": "Response should be under 100 words",
                "eval_method": "code"
            }
        ]
        self.sample_prompt_template = "Evaluate the following text: {text}"
        self.sample_vars_context = {"text": "Sample text to evaluate"}
        self.sample_bad_example = ["This is a bad example response"]

    @patch('dataplatform.evalgen.main.evalgen_wizard_model')
    @patch('dataplatform.evalgen.main.build_function_gen_prompt')
    def test_evalgen_criteria_processing_basic(self, mock_build_prompt, mock_wizard_model):
        """Test basic criteria processing functionality."""
        # Setup mocks
        mock_llm = MagicMock()
        mock_wizard_model.return_value = mock_llm
        mock_build_prompt.return_value = "Generated prompt"
        
        # Mock the chain output
        mock_output = {"result": "Generated evaluation function"}
        mock_chain = MagicMock()
        mock_chain.invoke.return_value = mock_output
        mock_llm.__or__ = MagicMock(return_value=mock_chain)
        
        # Execute function
        result = evalgen_criteria_processing(
            criterias=self.sample_criterias,
            prompt_template=self.sample_prompt_template,
            vars_context=self.sample_vars_context,
            bad_example=self.sample_bad_example
        )
        
        # Assertions
        assert len(result) == len(self.sample_criterias)
        assert all(isinstance(item, dict) for item in result)
        mock_wizard_model.assert_called_once_with(model="gpt-4o")
        assert mock_build_prompt.call_count == len(self.sample_criterias)

    @patch('dataplatform.evalgen.main.evalgen_wizard_model')
    @patch('dataplatform.evalgen.main.build_function_gen_prompt')
    def test_evalgen_criteria_processing_with_none_params(self, mock_build_prompt, mock_wizard_model):
        """Test criteria processing with None optional parameters."""
        # Setup mocks
        mock_llm = MagicMock()
        mock_wizard_model.return_value = mock_llm
        mock_build_prompt.return_value = "Generated prompt"
        
        mock_output = {"result": "Generated evaluation function"}
        mock_chain = MagicMock()
        mock_chain.invoke.return_value = mock_output
        mock_llm.__or__ = MagicMock(return_value=mock_chain)
        
        # Execute function with None parameters
        result = evalgen_criteria_processing(
            criterias=self.sample_criterias,
            prompt_template=self.sample_prompt_template,
            vars_context=None,
            bad_example=None
        )
        
        # Assertions
        assert len(result) == len(self.sample_criterias)
        # Verify that build_function_gen_prompt was called with None values
        for call_args in mock_build_prompt.call_args_list:
            args, kwargs = call_args
            assert kwargs['bad_example'] is None
            assert kwargs['vars_context'] is None

    @patch('dataplatform.evalgen.main.evalgen_wizard_model')
    @patch('dataplatform.evalgen.main.build_function_gen_prompt')
    def test_evalgen_criteria_processing_empty_criterias(self, mock_build_prompt, mock_wizard_model):
        """Test criteria processing with empty criterias list."""
        # Setup mocks
        mock_llm = MagicMock()
        mock_wizard_model.return_value = mock_llm
        
        # Execute function with empty criterias
        result = evalgen_criteria_processing(
            criterias=[],
            prompt_template=self.sample_prompt_template
        )
        
        # Assertions
        assert result == []
        mock_wizard_model.assert_called_once_with(model="gpt-4o")
        mock_build_prompt.assert_not_called()

    @patch('dataplatform.evalgen.main.evalgen_wizard_model')
    @patch('dataplatform.evalgen.main.build_function_gen_prompt')
    def test_evalgen_criteria_processing_missing_keys(self, mock_build_prompt, mock_wizard_model):
        """Test criteria processing with criterias missing keys."""
        # Setup mocks
        mock_llm = MagicMock()
        mock_wizard_model.return_value = mock_llm
        mock_build_prompt.return_value = "Generated prompt"
        
        mock_output = {"result": "Generated evaluation function"}
        mock_chain = MagicMock()
        mock_chain.invoke.return_value = mock_output
        mock_llm.__or__ = MagicMock(return_value=mock_chain)
        
        # Criterias with missing keys
        incomplete_criterias = [{"criteria": "Test criteria"}]  # Missing eval_method
        
        # Execute function
        result = evalgen_criteria_processing(
            criterias=incomplete_criterias,
            prompt_template=self.sample_prompt_template
        )
        
        # Assertions
        assert len(result) == 1
        # Verify that build_function_gen_prompt was called with empty string for missing eval_method
        mock_build_prompt.assert_called_once()
        call_args = mock_build_prompt.call_args
        assert call_args[1]['eval_method'] == ""


class TestEvalgenCriteriaGeneration:
    """Test cases for evalgen_criteria_generation function."""

    def setup_method(self):
        """Set up test data."""
        self.sample_prompt = "Generate a response about AI"
        self.sample_feedback_prompt = "Provide constructive feedback"
        self.sample_output_file = "test_criteria.json"

    @patch('dataplatform.evalgen.main.evalgen_wizard_model')
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.dump')
    @patch('dataplatform.evalgen.main.ChatPromptTemplate')
    def test_evalgen_criteria_generation_basic(self, mock_chat_template, mock_json_dump, mock_file, mock_wizard_model):
        """Test basic criteria generation functionality."""
        # Setup mocks
        mock_llm = MagicMock()
        mock_wizard_model.return_value = mock_llm
        
        mock_template = MagicMock()
        mock_chat_template.from_messages.return_value = mock_template
        
        mock_response = MagicMock()
        mock_response.content = "Generated criteria content"
        mock_chain = MagicMock()
        mock_chain.invoke.return_value = mock_response
        mock_template.__or__ = MagicMock(return_value=mock_chain)
        
        # Execute function
        evalgen_criteria_generation(
            prompt=self.sample_prompt,
            user_feedback_prompt=self.sample_feedback_prompt,
            output_file=self.sample_output_file
        )
        
        # Assertions
        mock_wizard_model.assert_called_once_with(model="gpt-4o")
        mock_chat_template.from_messages.assert_called_once()
        mock_chain.invoke.assert_called_once_with({
            "prompt": self.sample_prompt,
            "userFeedbackPrompt": self.sample_feedback_prompt,
        })
        mock_file.assert_called_once_with(f"./src/libraries/evalgen_src/docs/{self.sample_output_file}", "w")
        mock_json_dump.assert_called_once_with("Generated criteria content", mock_file(), indent=2)

    @patch('dataplatform.evalgen.main.evalgen_wizard_model')
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.dump')
    @patch('dataplatform.evalgen.main.ChatPromptTemplate')
    def test_evalgen_criteria_generation_empty_strings(self, mock_chat_template, mock_json_dump, mock_file, mock_wizard_model):
        """Test criteria generation with empty string inputs."""
        # Setup mocks
        mock_llm = MagicMock()
        mock_wizard_model.return_value = mock_llm
        
        mock_template = MagicMock()
        mock_chat_template.from_messages.return_value = mock_template
        
        mock_response = MagicMock()
        mock_response.content = ""
        mock_chain = MagicMock()
        mock_chain.invoke.return_value = mock_response
        mock_template.__or__ = MagicMock(return_value=mock_chain)
        
        # Execute function with empty strings
        evalgen_criteria_generation(
            prompt="",
            user_feedback_prompt="",
            output_file=self.sample_output_file
        )
        
        # Assertions
        mock_chain.invoke.assert_called_once_with({
            "prompt": "",
            "userFeedbackPrompt": "",
        })
        mock_json_dump.assert_called_once_with("", mock_file(), indent=2)

    @patch('dataplatform.evalgen.main.evalgen_wizard_model')
    @patch('builtins.open', side_effect=IOError("File write error"))
    @patch('dataplatform.evalgen.main.ChatPromptTemplate')
    def test_evalgen_criteria_generation_file_error(self, mock_chat_template, mock_file, mock_wizard_model):
        """Test criteria generation when file writing fails."""
        # Setup mocks
        mock_llm = MagicMock()
        mock_wizard_model.return_value = mock_llm
        
        mock_template = MagicMock()
        mock_chat_template.from_messages.return_value = mock_template
        
        mock_response = MagicMock()
        mock_response.content = "Generated criteria content"
        mock_chain = MagicMock()
        mock_chain.invoke.return_value = mock_response
        mock_template.__or__ = MagicMock(return_value=mock_chain)
        
        # Execute function and expect IOError
        with pytest.raises(IOError, match="File write error"):
            evalgen_criteria_generation(
                prompt=self.sample_prompt,
                user_feedback_prompt=self.sample_feedback_prompt,
                output_file=self.sample_output_file
            )