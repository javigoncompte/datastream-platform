"""Tests for evalgen.test_cases module."""

import pytest
from unittest.mock import patch, MagicMock
from dataplatform.evalgen.test_cases import (
    TestCase,
    get_test_case_output,
    generate_test_case_prompt,
    generate_test_case,
    process_test_cases,
    get_output_from_agent,
)


class TestTestCaseModel:
    """Test cases for the TestCase Pydantic model."""

    def test_test_case_creation_minimal(self):
        """Test creating a TestCase with minimal required fields."""
        test_case = TestCase(
            feature="Login",
            scenario="Valid credentials",
            persona="Regular user"
        )
        
        assert test_case.feature == "Login"
        assert test_case.scenario == "Valid credentials"
        assert test_case.persona == "Regular user"
        assert test_case.constraints == []
        assert test_case.assumptions == []
        assert test_case.test_case_prompt is None
        assert test_case.test_case_output is None

    def test_test_case_creation_full(self):
        """Test creating a TestCase with all fields."""
        test_case = TestCase(
            feature="Order Processing",
            scenario="High volume orders",
            persona="Enterprise customer",
            constraints=["Must complete within 5 seconds"],
            assumptions=["Database is available"],
            test_case_prompt="Generate order test",
            test_case_output="Order processed successfully"
        )
        
        assert test_case.feature == "Order Processing"
        assert test_case.scenario == "High volume orders"
        assert test_case.persona == "Enterprise customer"
        assert test_case.constraints == ["Must complete within 5 seconds"]
        assert test_case.assumptions == ["Database is available"]
        assert test_case.test_case_prompt == "Generate order test"
        assert test_case.test_case_output == "Order processed successfully"

    def test_test_case_model_dump_json(self):
        """Test TestCase JSON serialization."""
        test_case = TestCase(
            feature="API",
            scenario="Rate limiting",
            persona="Developer"
        )
        
        json_str = test_case.model_dump_json()
        assert "API" in json_str
        assert "Rate limiting" in json_str
        assert "Developer" in json_str


class TestGetTestCaseOutput:
    """Test cases for get_test_case_output function."""

    @patch('dataplatform.evalgen.test_cases.mlflow')
    @patch('dataplatform.evalgen.test_cases.get_deploy_client')
    def test_get_test_case_output_success(self, mock_get_deploy_client, mock_mlflow):
        """Test successful test case output generation."""
        # Setup mocks
        mock_deploy_client = MagicMock()
        mock_get_deploy_client.return_value = mock_deploy_client
        mock_deploy_client.__class__ = MagicMock()
        mock_deploy_client.__class__.__name__ = 'DatabricksDeploymentClient'
        
        # Mock response
        mock_response = {
            "choices": [
                {"message": {"content": "Test response content"}}
            ]
        }
        mock_deploy_client.predict.return_value = mock_response
        
        # Execute function
        result = get_test_case_output(
            endpoint_name="test-endpoint",
            prompt="Test prompt",
            experiment_name="test-experiment"
        )
        
        # Assertions
        assert result == "Test response content"
        mock_get_deploy_client.assert_called_once_with("databricks")
        mock_deploy_client.predict.assert_called_once()
        mock_mlflow.log_param.assert_any_call("prompt", "Test prompt")
        mock_mlflow.log_param.assert_any_call("experiment_name", "test-experiment")

    @patch('dataplatform.evalgen.test_cases.mlflow')
    @patch('dataplatform.evalgen.test_cases.get_deploy_client')
    def test_get_test_case_output_none_client(self, mock_get_deploy_client, mock_mlflow):
        """Test get_test_case_output when deploy client is None."""
        mock_get_deploy_client.return_value = None
        
        with pytest.raises(ValueError, match="Deploy Client has not been set"):
            get_test_case_output(
                endpoint_name="test-endpoint",
                prompt="Test prompt",
                experiment_name="test-experiment"
            )

    @patch('dataplatform.evalgen.test_cases.mlflow')
    @patch('dataplatform.evalgen.test_cases.get_deploy_client')
    def test_get_test_case_output_unsupported_client(self, mock_get_deploy_client, mock_mlflow):
        """Test get_test_case_output with unsupported client type."""
        mock_deploy_client = MagicMock()
        mock_get_deploy_client.return_value = mock_deploy_client
        mock_deploy_client.__class__ = type('UnsupportedClient', (), {})
        
        with pytest.raises(NotImplementedError, match="Deploy Client type"):
            get_test_case_output(
                endpoint_name="test-endpoint",
                prompt="Test prompt",
                experiment_name="test-experiment"
            )


class TestGenerateTestCasePrompt:
    """Test cases for generate_test_case_prompt function."""

    def setup_method(self):
        """Set up test data."""
        self.sample_test_case = TestCase(
            feature="Order Tracking",
            scenario="Invalid Data Provided",
            persona="Frustrated Customer",
            assumptions=["Order number #1234 does not exist in the system"],
            constraints=["Be professional and concise"]
        )

    @patch('dataplatform.evalgen.test_cases.ChatDatabricks')
    @patch('dataplatform.evalgen.test_cases.ChatPromptTemplate')
    @patch('dataplatform.evalgen.test_cases.FewShotChatMessagePromptTemplate')
    def test_generate_test_case_prompt_success(self, mock_few_shot, mock_chat_template, mock_chat_databricks):
        """Test successful test case prompt generation."""
        # Setup mocks
        mock_llm = MagicMock()
        mock_chat_databricks.return_value = mock_llm
        
        mock_prompt = MagicMock()
        mock_chat_template.from_messages.return_value = mock_prompt
        
        mock_response = MagicMock()
        mock_response.content = "Generated test case prompt"
        mock_chain = MagicMock()
        mock_chain.invoke.return_value = mock_response
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        
        # Execute function
        result = generate_test_case_prompt(
            model="test-model",
            test_case=self.sample_test_case,
            temperature=0.7,
            max_tokens=1000,
            top_p=1.0
        )
        
        # Assertions
        assert result == "Generated test case prompt"
        mock_chat_databricks.assert_called_once_with(
            model="test-model",
            temperature=0.7,
            max_tokens=1000,
            top_p=1.0
        )
        mock_chain.invoke.assert_called_once()

    @patch('dataplatform.evalgen.test_cases.ChatDatabricks')
    @patch('dataplatform.evalgen.test_cases.ChatPromptTemplate')
    @patch('dataplatform.evalgen.test_cases.FewShotChatMessagePromptTemplate')
    def test_generate_test_case_prompt_default_params(self, mock_few_shot, mock_chat_template, mock_chat_databricks):
        """Test generate_test_case_prompt with default parameters."""
        # Setup mocks
        mock_llm = MagicMock()
        mock_chat_databricks.return_value = mock_llm
        
        mock_prompt = MagicMock()
        mock_chat_template.from_messages.return_value = mock_prompt
        
        mock_response = MagicMock()
        mock_response.content = "Generated test case prompt"
        mock_chain = MagicMock()
        mock_chain.invoke.return_value = mock_response
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        
        # Execute function with minimal parameters
        result = generate_test_case_prompt(
            model="test-model",
            test_case=self.sample_test_case
        )
        
        # Assertions
        assert result == "Generated test case prompt"
        mock_chat_databricks.assert_called_once_with(
            model="test-model",
            temperature=0.7,  # default
            max_tokens=1000,  # default
            top_p=1.0  # default
        )


class TestGenerateTestCase:
    """Test cases for generate_test_case function."""

    def setup_method(self):
        """Set up test data."""
        self.sample_test_case = TestCase(
            feature="Login",
            scenario="Invalid credentials",
            persona="Impatient user"
        )

    @patch('dataplatform.evalgen.test_cases.generate_test_case_prompt')
    @patch('dataplatform.evalgen.test_cases.get_test_case_output')
    def test_generate_test_case_success(self, mock_get_output, mock_generate_prompt):
        """Test successful test case generation."""
        # Setup mocks
        mock_generate_prompt.return_value = "Generated prompt"
        mock_get_output.return_value = "Generated output"
        
        # Execute function
        result = generate_test_case(
            test_case=self.sample_test_case,
            experiment_name="test-experiment",
            endpoint_name="test-endpoint",
            model_for_test_case_prompt="test-model"
        )
        
        # Assertions
        assert result.test_case_prompt == "Generated prompt"
        assert result.test_case_output == "Generated output"
        assert result.feature == self.sample_test_case.feature
        assert result.scenario == self.sample_test_case.scenario
        assert result.persona == self.sample_test_case.persona
        
        mock_generate_prompt.assert_called_once_with(
            model="test-model",
            test_case=self.sample_test_case
        )
        mock_get_output.assert_called_once_with(
            "test-endpoint",
            "Generated prompt",
            "test-experiment"
        )

    @patch('dataplatform.evalgen.test_cases.generate_test_case_prompt')
    @patch('dataplatform.evalgen.test_cases.get_test_case_output')
    def test_generate_test_case_default_model(self, mock_get_output, mock_generate_prompt):
        """Test generate_test_case with default model."""
        # Setup mocks
        mock_generate_prompt.return_value = "Generated prompt"
        mock_get_output.return_value = "Generated output"
        
        # Execute function without specifying model
        result = generate_test_case(
            test_case=self.sample_test_case,
            experiment_name="test-experiment",
            endpoint_name="test-endpoint"
        )
        
        # Assertions
        mock_generate_prompt.assert_called_once_with(
            model="open-ai-gpt-4o-endpoint",  # default
            test_case=self.sample_test_case
        )


class TestProcessTestCases:
    """Test cases for process_test_cases function."""

    def setup_method(self):
        """Set up test data."""
        self.sample_test_cases = [
            TestCase(
                feature="Feature1",
                scenario="Scenario1",
                persona="Persona1"
            ),
            TestCase(
                feature="Feature2",
                scenario="Scenario2",
                persona="Persona2"
            )
        ]

    @patch('dataplatform.evalgen.test_cases.init_mlflow')
    @patch('dataplatform.evalgen.test_cases.mlflow')
    @patch('dataplatform.evalgen.test_cases.generate_test_case')
    def test_process_test_cases_success(self, mock_generate_test_case, mock_mlflow, mock_init_mlflow):
        """Test successful processing of test cases."""
        # Setup mocks
        mock_experiment = MagicMock()
        mock_experiment.experiment_id = "test-experiment-id"
        mock_init_mlflow.return_value = mock_experiment
        
        mock_run = MagicMock()
        mock_run.info.run_id = "test-run-id"
        mock_mlflow.start_run.return_value.__enter__.return_value = mock_run
        
        updated_test_case_1 = TestCase(
            feature="Feature1",
            scenario="Scenario1",
            persona="Persona1",
            test_case_prompt="Generated prompt 1",
            test_case_output="Generated output 1"
        )
        updated_test_case_2 = TestCase(
            feature="Feature2",
            scenario="Scenario2",
            persona="Persona2",
            test_case_prompt="Generated prompt 2",
            test_case_output="Generated output 2"
        )
        
        mock_generate_test_case.side_effect = [updated_test_case_1, updated_test_case_2]
        
        # Execute function
        result_test_cases, run_id = process_test_cases(
            test_cases=self.sample_test_cases,
            experiment_name="test-experiment",
            endpoint_name="test-endpoint"
        )
        
        # Assertions
        assert len(result_test_cases) == 2
        assert run_id == "test-run-id"
        assert result_test_cases[0].test_case_prompt == "Generated prompt 1"
        assert result_test_cases[1].test_case_prompt == "Generated prompt 2"
        
        mock_init_mlflow.assert_called_once_with("test-experiment")
        assert mock_generate_test_case.call_count == 2

    @patch('dataplatform.evalgen.test_cases.init_mlflow')
    @patch('dataplatform.evalgen.test_cases.mlflow')
    @patch('dataplatform.evalgen.test_cases.generate_test_case')
    def test_process_test_cases_empty_list(self, mock_generate_test_case, mock_mlflow, mock_init_mlflow):
        """Test processing empty test cases list."""
        # Setup mocks
        mock_experiment = MagicMock()
        mock_experiment.experiment_id = "test-experiment-id"
        mock_init_mlflow.return_value = mock_experiment
        
        mock_run = MagicMock()
        mock_run.info.run_id = "test-run-id"
        mock_mlflow.start_run.return_value.__enter__.return_value = mock_run
        
        # Execute function with empty list
        result_test_cases, run_id = process_test_cases(
            test_cases=[],
            experiment_name="test-experiment",
            endpoint_name="test-endpoint"
        )
        
        # Assertions
        assert len(result_test_cases) == 0
        assert run_id == "test-run-id"
        mock_generate_test_case.assert_not_called()


class TestGetOutputFromAgent:
    """Test cases for get_output_from_agent function."""

    def setup_method(self):
        """Set up test data."""
        self.sample_test_cases = [
            TestCase(
                feature="Feature1",
                scenario="Scenario1",
                persona="Persona1",
                test_case_output="Test output 1"
            ),
            TestCase(
                feature="Feature2",
                scenario="Scenario2",
                persona="Persona2",
                test_case_output="Test output 2"
            )
        ]

    @patch('dataplatform.evalgen.test_cases.init_mlflow')
    @patch('dataplatform.evalgen.test_cases.mlflow')
    def test_get_output_from_agent_with_version(self, mock_mlflow, mock_init_mlflow):
        """Test get_output_from_agent with version specified."""
        # Setup mocks
        mock_experiment = MagicMock()
        mock_experiment.experiment_id = "test-experiment-id"
        mock_init_mlflow.return_value = mock_experiment
        
        mock_run = MagicMock()
        mock_run.info.run_id = "test-run-id"
        mock_mlflow.start_run.return_value.__enter__.return_value = mock_run
        
        mock_agent = MagicMock()
        mock_agent.predict.side_effect = [
            {"messages": [{"content": "Agent response 1"}]},
            {"messages": [{"content": "Agent response 2"}]}
        ]
        mock_mlflow.pyfunc.load_model.return_value = mock_agent
        
        # Execute function
        responses, run_id = get_output_from_agent(
            test_cases=self.sample_test_cases,
            agent_name="test-agent",
            version="1"
        )
        
        # Assertions
        assert len(responses) == 2
        assert responses[0] == "Agent response 1"
        assert responses[1] == "Agent response 2"
        assert run_id == "test-run-id"
        
        mock_mlflow.pyfunc.load_model.assert_called_once_with("models://test-agent/1")
        assert mock_agent.predict.call_count == 2

    @patch('dataplatform.evalgen.test_cases.init_mlflow')
    @patch('dataplatform.evalgen.test_cases.mlflow')
    def test_get_output_from_agent_with_alias(self, mock_mlflow, mock_init_mlflow):
        """Test get_output_from_agent with alias specified."""
        # Setup mocks
        mock_experiment = MagicMock()
        mock_experiment.experiment_id = "test-experiment-id"
        mock_init_mlflow.return_value = mock_experiment
        
        mock_run = MagicMock()
        mock_run.info.run_id = "test-run-id"
        mock_mlflow.start_run.return_value.__enter__.return_value = mock_run
        
        mock_client = MagicMock()
        mock_version = MagicMock()
        mock_version.version = "2"
        mock_client.get_model_version_by_alias.return_value = mock_version
        mock_mlflow.MlflowClient.return_value = mock_client
        
        mock_agent = MagicMock()
        mock_agent.predict.return_value = {"messages": [{"content": "Agent response"}]}
        mock_mlflow.pyfunc.load_model.return_value = mock_agent
        
        # Execute function
        responses, run_id = get_output_from_agent(
            test_cases=self.sample_test_cases[:1],  # Use only one test case
            agent_name="test-agent",
            alias="production"
        )
        
        # Assertions
        assert len(responses) == 1
        assert responses[0] == "Agent response"
        mock_client.get_model_version_by_alias.assert_called_once_with("test-agent", "production")
        mock_mlflow.pyfunc.load_model.assert_called_once_with("models://test-agent/2")

    @patch('dataplatform.evalgen.test_cases.init_mlflow')
    @patch('dataplatform.evalgen.test_cases.mlflow')
    def test_get_output_from_agent_none_response(self, mock_mlflow, mock_init_mlflow):
        """Test get_output_from_agent when agent returns None response."""
        # Setup mocks
        mock_experiment = MagicMock()
        mock_experiment.experiment_id = "test-experiment-id"
        mock_init_mlflow.return_value = mock_experiment
        
        mock_run = MagicMock()
        mock_run.info.run_id = "test-run-id"
        mock_mlflow.start_run.return_value.__enter__.return_value = mock_run
        
        mock_agent = MagicMock()
        mock_agent.predict.return_value = None
        mock_mlflow.pyfunc.load_model.return_value = mock_agent
        
        # Execute function
        responses, run_id = get_output_from_agent(
            test_cases=self.sample_test_cases[:1],
            agent_name="test-agent",
            version="1"
        )
        
        # Assertions
        assert len(responses) == 1
        assert "No response from agent for test case" in responses[0]
        assert run_id == "test-run-id"