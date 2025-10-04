#!/usr/bin/env -S uv run --script
# A test script to run the evalgen pipeline on one testcase with a specific agent
# This creates a labeling session for annotation of the testcase output

from evalgen.label import process_annotation_session
from evalgen.test_cases import (
    TestCase,
    get_output_from_agent,
    process_test_cases,
)

if __name__ == "__main__":
    test_cases = [
        TestCase(
            feature="Planning a trip",
            scenario="Reservation for a trip 642693",
            constraints=[],
            persona="New Destination Planner",
            assumptions=["Reservation for this trip exists"],
        )
    ]
    test_cases, run_id = process_test_cases(
        test_cases,
        experiment_name="/experiments/evalgen_annotation",
        endpoint_name="open-ai-gpt-4o-endpoint",
    )
    responses, run_id = get_output_from_agent(
        test_cases,
        "qa_agents.members.trip_planner",
        alias="Champion",
    )
    annotation_session = process_annotation_session(
        agent_name="trip_planner_champion", run_id=run_id
    )
