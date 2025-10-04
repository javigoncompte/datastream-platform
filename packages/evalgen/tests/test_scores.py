"""Tests for evalgen.scores module."""

import pytest
from unittest.mock import patch
from dataplatform.evalgen.scores import (
    get_f1_score,
    get_cohen_kappa_score,
    get_matthews_corrcoef,
    get_accuracy_score,
)


class TestScoresFunctions:
    """Test cases for scoring functions."""

    def setup_method(self):
        """Set up test data."""
        # Perfect agreement
        self.perfect_labels = [1, 1, 0, 0, 1]
        self.perfect_predictions = [1, 1, 0, 0, 1]
        
        # Moderate agreement
        self.moderate_expert = [1, 1, 0, 0, 1, 0, 1]
        self.moderate_llm = [1, 0, 0, 1, 1, 0, 0]
        
        # No agreement
        self.no_agreement_expert = [1, 1, 1, 1]
        self.no_agreement_llm = [0, 0, 0, 0]
        
        # Edge case - all same class
        self.all_ones_expert = [1, 1, 1, 1]
        self.all_ones_llm = [1, 1, 1, 1]

    def test_get_f1_score_perfect_agreement(self):
        """Test F1 score with perfect agreement."""
        score = get_f1_score(self.perfect_labels, self.perfect_predictions)
        assert score == 1.0
        assert isinstance(score, float)

    def test_get_f1_score_moderate_agreement(self):
        """Test F1 score with moderate agreement."""
        score = get_f1_score(self.moderate_expert, self.moderate_llm)
        assert 0.0 <= score <= 1.0
        assert isinstance(score, float)

    def test_get_f1_score_no_agreement(self):
        """Test F1 score with no agreement."""
        score = get_f1_score(self.no_agreement_expert, self.no_agreement_llm)
        assert score == 0.0
        assert isinstance(score, float)

    def test_get_cohen_kappa_score_perfect_agreement(self):
        """Test Cohen's Kappa with perfect agreement."""
        score = get_cohen_kappa_score(self.perfect_labels, self.perfect_predictions)
        assert score == 1.0
        assert isinstance(score, float)

    def test_get_cohen_kappa_score_moderate_agreement(self):
        """Test Cohen's Kappa with moderate agreement."""
        score = get_cohen_kappa_score(self.moderate_expert, self.moderate_llm)
        assert -1.0 <= score <= 1.0
        assert isinstance(score, float)

    def test_get_cohen_kappa_score_all_same_class(self):
        """Test Cohen's Kappa when all predictions are the same class."""
        score = get_cohen_kappa_score(self.all_ones_expert, self.all_ones_llm)
        # When all labels are the same, sklearn returns NaN for Cohen's Kappa
        # This is expected behavior as the score is undefined in this case
        import math
        assert math.isnan(score) or score == 1.0
        assert isinstance(score, float)

    def test_get_matthews_corrcoef_perfect_agreement(self):
        """Test Matthews correlation coefficient with perfect agreement."""
        score = get_matthews_corrcoef(self.perfect_labels, self.perfect_predictions)
        assert score == 1.0
        assert isinstance(score, float)

    def test_get_matthews_corrcoef_moderate_agreement(self):
        """Test Matthews correlation coefficient with moderate agreement."""
        score = get_matthews_corrcoef(self.moderate_expert, self.moderate_llm)
        assert -1.0 <= score <= 1.0
        assert isinstance(score, float)

    def test_get_matthews_corrcoef_no_agreement(self):
        """Test Matthews correlation coefficient with no agreement."""
        score = get_matthews_corrcoef(self.no_agreement_expert, self.no_agreement_llm)
        assert -1.0 <= score <= 1.0
        assert isinstance(score, float)

    def test_get_accuracy_score_perfect_agreement(self):
        """Test accuracy score with perfect agreement."""
        score = get_accuracy_score(self.perfect_labels, self.perfect_predictions)
        assert score == 1.0
        assert isinstance(score, float)

    def test_get_accuracy_score_moderate_agreement(self):
        """Test accuracy score with moderate agreement."""
        score = get_accuracy_score(self.moderate_expert, self.moderate_llm)
        assert 0.0 <= score <= 1.0
        assert isinstance(score, float)

    def test_get_accuracy_score_no_agreement(self):
        """Test accuracy score with no agreement."""
        score = get_accuracy_score(self.no_agreement_expert, self.no_agreement_llm)
        assert score == 0.0
        assert isinstance(score, float)

    def test_empty_lists_behavior(self):
        """Test behavior with empty lists - sklearn returns 0.0 with warnings."""
        import warnings
        import math
        
        # sklearn doesn't raise ValueError for empty lists, behavior varies by metric
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            assert get_f1_score([], []) == 0.0
            # Cohen's kappa returns NaN for empty lists
            kappa_score = get_cohen_kappa_score([], [])
            assert math.isnan(kappa_score) or kappa_score == 0.0
            assert get_matthews_corrcoef([], []) == 0.0
            assert get_accuracy_score([], []) == 0.0

    def test_mismatched_length_lists_raise_error(self):
        """Test that mismatched length lists raise appropriate errors."""
        short_list = [1, 0]
        long_list = [1, 0, 1, 0]
        
        with pytest.raises(ValueError):
            get_f1_score(short_list, long_list)
        
        with pytest.raises(ValueError):
            get_cohen_kappa_score(short_list, long_list)
        
        with pytest.raises(ValueError):
            get_matthews_corrcoef(short_list, long_list)
        
        with pytest.raises(ValueError):
            get_accuracy_score(short_list, long_list)

    @pytest.mark.parametrize("expert_labels,llm_labels,expected_accuracy", [
        ([1, 1, 0, 0], [1, 1, 0, 0], 1.0),
        ([1, 0, 1, 0], [1, 1, 1, 1], 0.5),
        ([0, 0, 0, 0], [1, 1, 1, 1], 0.0),
    ])
    def test_accuracy_score_parametrized(self, expert_labels, llm_labels, expected_accuracy):
        """Test accuracy score with parametrized inputs."""
        score = get_accuracy_score(expert_labels, llm_labels)
        assert score == expected_accuracy