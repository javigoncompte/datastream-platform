from sklearn.metrics import (
    accuracy_score,
    cohen_kappa_score,
    f1_score,
    matthews_corrcoef,
)


def get_f1_score(
    domain_expert_labels: list[int], llm_labels: list[int]
) -> float:
    return float(f1_score(domain_expert_labels, llm_labels))


def get_cohen_kappa_score(
    domain_expert_labels: list[int], llm_labels: list[int]
) -> float:
    return float(cohen_kappa_score(domain_expert_labels, llm_labels))


def get_matthews_corrcoef(
    domain_expert_labels: list[int], llm_labels: list[int]
) -> float:
    return float(matthews_corrcoef(domain_expert_labels, llm_labels))


def get_accuracy_score(
    domain_expert_labels: list[int], llm_labels: list[int]
) -> float:
    return float(accuracy_score(domain_expert_labels, llm_labels))
