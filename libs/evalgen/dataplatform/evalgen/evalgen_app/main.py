import fasthtml.common as fh
import monsterui.all as mui
from db import Annotation, get_db, get_serving_endpoints, get_tables

app, rt = fh.fast_app(hdrs=tuple(mui.Theme.blue.headers()), live=True, debug=True)


@rt
def index():
    serving_endpoints = get_serving_endpoints()

    return mui.Container(
        mui.H1("Evalgen"),
        mui.Container(
            mui.DivLAligned(
                mui.Button("Home", href="/", cls=mui.ButtonT.primary),
                mui.Button(
                    "Evaluations",
                    href="/evaluations",
                    cls=mui.ButtonT.secondary,
                ),
                cls="flex gap-4 mb-8",
            ),
            mui.DivHStacked(
                fh.Div(
                    mui.Card(
                        mui.H3("Select A.I application"),
                        fh.Div(
                            mui.Form(
                                mui.LabelInput("Catalog", id="catalog"),
                                mui.LabelInput(
                                    "Schema",
                                    id="schema",
                                ),
                                mui.LabelInput(
                                    "Agent",
                                    id="agent",
                                ),
                                mui.Button(
                                    "Submit", type="submit", cls=mui.ButtonT.primary
                                ),
                                hx_post=instantiate_table.to(
                                    application_name="${catalog}.${schema}.${agent}"
                                ),
                            ),
                        ),
                        mui.Card(
                            mui.H3("Select LLM for synthetic data generation"),
                            fh.Div(
                                mui.Button(
                                    "Select Agent/LLM",
                                    cls=mui.ButtonT.secondary,
                                    id="agent-selector-btn",
                                ),
                                mui.DropDownNavContainer(
                                    *[
                                        mui.NavCloseLi(
                                            fh.A(
                                                mui.DivFullySpaced(
                                                    fh.P(serving_endpoint.name),
                                                    fh.P(
                                                        serving_endpoint.name,
                                                        cls=mui.TextPresets.muted_sm,
                                                    ),
                                                ),
                                                hx_post=select_agent.to(
                                                    agent_name=serving_endpoint.name
                                                ),
                                                hx_target="#agent-selector-btn",
                                                hx_swap="outerHTML",
                                            )
                                        )
                                        for serving_endpoint in serving_endpoints
                                    ],
                                    cls=mui.NavT.primary,
                                ),
                            ),
                            cls="h-fit",
                        ),
                        cls="flex-1 space-y-4",
                    ),
                    cls="gap-6 items-start",
                ),
            ),
        ),
    )


@rt
def evaluations():
    """Show all available evaluations."""
    return mui.Container(
        mui.H1("Evaluations"),
        mui.Container(
            mui.Button("Home", href="/", cls=mui.ButtonT.secondary),
            mui.Button("Evaluations", href="/evaluations", cls=mui.ButtonT.primary),
            cls="flex gap-4 mb-8",
        ),
    )


@rt
def instantiate_table(application_name: str, session):
    """Handle table instantiation with the provided table name."""
    get_tables(application_name)
    session.setdefault("application_name", application_name)
    return ""


@rt
def evaluate_index(session):
    """Show evaluation index for the table."""
    table_name = session.get("table_name")
    if not table_name:
        return fh.RedirectResponse("/")

    db = get_db(table_name)
    body = []
    unique_inputs = list(db.annotations.rows_where(select="distinct input_id, input"))
    for unique_input in unique_inputs:
        body.append({
            "Input": f"{unique_input['input'][:125]}...",
            "Action": fh.A(
                "Evaluate",
                cls=mui.AT.primary,
                href=evaluate.to(input_id=unique_input["input_id"]),
            ),
        })

    return mui.Container(
        mui.H1(f"Evaluation Index - {table_name}"),
        mui.TableFromDicts(["Input", "Action"], body),
    )


@rt
def evaluate(input_id: str, session):
    table_name = session["table_name"]
    db = get_db(table_name)
    test_cases = list(db.test_cases.rows_where("input_id=?", [input_id]))
    body = []
    for test_case in test_cases:
        body.append({
            "Output": mui.render_md(test_case["document"]),
            "Notes": fh.Input(
                value=test_case["notes"],
                cls="min-w-96",
                name="notes",
                hx_post=update_notes.to(
                    input_id=input_id,
                    test_case_id=test_case["test_case_id"],
                ),
                hx_trigger="change",
            ),
            "Evaluation": eval_buttons(input_id, test_case["test_case_id"], table_name),
        })

    return mui.Container(
        fh.H1("Evaluating Input"),
        mui.Card(mui.render_md(test_cases[0]["input"].replace("\\n", "\n"))),
        mui.TableFromDicts(["Output", "Notes", "Evaluation"], body),
    )


@rt
def evaluate_doc(input_id: str, test_case_id: str, eval_type: str, session):
    table_name = session["table_name"]
    db = get_db(table_name)
    db.annotations.update(
        Annotation(input_id=input_id, test_case_id=test_case_id, eval_type=eval_type)
    )
    return eval_buttons(input_id, test_case_id, table_name)


@rt
def update_notes(input_id: str, test_case_id: str, notes: str, session):
    table_name = session["table_name"]
    db = get_db(table_name)
    record = Annotation(input_id=input_id, test_case_id=test_case_id, notes=notes)
    db.annotations.update(record)


def eval_buttons(input_id: str, test_case_id: str, table_name: str):
    db = get_db(table_name)
    target_id = (
        f"#eval-{input_id}-{test_case_id}"
        if test_case_id is not None
        else f"#eval-{input_id}"
    )
    _annotation = db.annotations[input_id, test_case_id]

    def create_eval_button(label: str):
        """Create an evaluation button with consistent properties."""
        return fh.Button(
            label,
            hx_post=evaluate_doc.to(
                input_id=input_id,
                test_case_id=test_case_id,
                eval_type=label.lower(),
                table_name=table_name,
            ),
            hx_target=target_id,
            cls=mui.ButtonT.primary
            if _annotation.eval_type == label.lower()
            else mui.ButtonT.secondary,
            submit=False,
        )

    return mui.DivLAligned(
        create_eval_button("Good"), create_eval_button("Bad"), id=target_id[1:]
    )


@rt
def select_agent(agent_name: str):
    """Handle agent selection and return updated button."""
    return mui.Button(
        f"Selected: {agent_name}",
        cls=mui.ButtonT.primary,
        id="agent-selector-btn",
    )


@rt
def save_dimensions(
    features: str, scenarios: str, constraints: str, personas: str, session
):
    """Save the test dimensions."""
    dimensions = Dimensions(
        features=features,
        scenarios=scenarios,
        constraints=constraints,
        personas=personas,
    )

    # Store dimensions in session for later use
    session["dimensions"] = dimensions.model_dump()

    # Create table data for display
    table_data = [
        {"Dimension": "Features", "Value": features},
        {"Dimension": "Scenarios", "Value": scenarios},
        {"Dimension": "Constraints", "Value": constraints},
        {"Dimension": "Personas", "Value": personas},
    ]

    # Success message
    success_msg = mui.Card(
        mui.H4("✅ Dimensions Saved", cls="text-green-500"),
        fh.P("Test dimensions have been saved successfully!"),
        cls="mt-2",
    )

    # Updated table
    dimensions_table = fh.Div(
        mui.H4("Current Test Dimensions", cls="mb-2"),
        mui.TableFromDicts(["Dimension", "Value"], table_data, cls=mui.TableT.striped),
        hx_swap_oob="true",
        id="dimensions-table",
    )

    return success_msg, dimensions_table


fh.serve()
