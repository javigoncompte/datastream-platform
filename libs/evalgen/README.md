# Evalgen

## Background and Motivation

### Sources:
- [ChainForge: A Visual Toolkit for Prompt Engineering and LLM Hypothesis Testing](https://arxiv.org/abs/2309.09128)
- [Who Validates the Validators? Aligning LLM-Assisted Evaluation of LLM Outputs with Human Preferences](https://arxiv.org/abs/2404.12272)

Traditional LLM evaluation pipelines typically involve generating outputs from a prompt under test and then applying a set of static metrics—either code-based or LLM-based evaluators—to produce test results. While this approach is straightforward, it often lacks flexibility and fails to capture nuanced alignment or evolving reasoning criteria.

**EvalGen** addresses these limitations by introducing a dynamic, criteria-driven evaluation architecture. Instead of relying solely on fixed metrics, EvalGen enables users to:

- **Iteratively define and refine evaluation criteria** using both LLMs and human input.
- **Generate candidate assertions** (testable statements about LLM outputs) with LLM assistance.
- **Select and filter assertions** to focus on those most relevant to the evaluation goals.
- **Grade LLM outputs** against these assertions, producing a detailed alignment report card.

This process creates a feedback loop where criteria and assertions can be edited and improved, allowing for more robust, context-aware, and transparent evaluation of LLM behavior. The result is a flexible evaluation pipeline that better aligns with real-world requirements and supports continuous improvement of both prompts and models.

![Workflow](./docs/evalgen_workflow.png)
