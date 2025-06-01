import fasthtml.common as fh
import monsterui.all as mui
from db import Annotation, get_db, get_serving_endpoints, get_tables

app, rt = fh.fast_app(hdrs=tuple(mui.Theme.zinc.headers()), live=True, debug=True)

header = mui.NavBar(
    fh.A("Home", href="/", cls=mui.AT.text),
    fh.A("Evaluations", href="/evaluations", cls=mui.AT.text),
    brand=mui.H3("Evalgen"),
    cls="flex-wrap gap-4 mb-8",
)

form = mui.Form(
    mui.LabelInput("Catalog", id="catalog"),
    mui.LabelInput("Schema", id="schema"),
    mui.LabelInput("Agent", id="agent"),
    mui.Button("Submit", type="submit", cls=mui.ButtonT.primary),
)


DISPLAY_COLUMNS = [
    "input_id",
    "test_case_id",
    "input",
    "output",
    "notes",
    "eval_type",
    "features",
    "scenarios",
    "constraints",
    "personas",
    "assumptions",
    "generate_test_case_prompt",
    "agent_input",
    "agent_output",
]


def get_next_input_id(session) -> int:
    test_cases = session.get("test_cases", [])
    if not test_cases:
        return 1
    return max(tc.get("input_id", 0) for tc in test_cases) + 1


def test_case_table(session):
    table_header_display = DISPLAY_COLUMNS + ["Actions"]

    test_cases_data = session.get("test_cases", [])
    table_body = []
    for tc in test_cases_data:
        row = {col: tc.get(col, "") for col in DISPLAY_COLUMNS}
        row["Actions"] = fh.Div(
            mui.Button(
                "Edit",
                cls=mui.ButtonT.secondary,
                hx_get=f"/edit_test_case_form/{tc['input_id']}",
                hx_target=f"#row-{tc['input_id']}",
                hx_swap="outerHTML",
            ),
            cls="space-x-2",
        )
        table_body.append(
            fh.Tr(id=f"row-{tc['input_id']}")(*[
                fh.Td(str(row[col])) for col in table_header_display
            ])
        )

    add_button = mui.Button(
        "Add New Test Case",
        cls=mui.ButtonT.primary,
        hx_get="/add_test_case_row_form",
        hx_target="#test-case-table-body",
        hx_swap="beforeend",
    )

    return fh.Div(
        add_button,
        mui.Table(
            fh.Thead(
                fh.Tr(*[
                    fh.Th(col_name.replace("_", " ").title())
                    for col_name in table_header_display
                ])
            ),
            fh.Tbody(*table_body, id="test-case-table-body"),
            cls=[
                mui.TableT.striped,
                mui.TableT.hover,
                mui.TableT.responsive,
                "w-full mt-4",
            ],
        ),
        id="test-case-table-container",
    )


@rt
def index(session):
    serving_endpoints = get_serving_endpoints()
    return (
        mui.Container(
            header,
            mui.Container(
                mui.DivHStacked(
                    fh.Div(
                        mui.Card(
                            mui.H3("Select A.I application"),
                            fh.Div(
                                form,
                                hx_post=instantiate_table.to(
                                    application_name="${catalog}.${schema}.${agent}"
                                ),
                                hx_target="#ai-application-btn",
                                hx_swap="outerHTML",
                            ),
                            mui.H5(
                                "A.I Application",
                                cls=mui.TextPresets.bold_lg,
                                id="ai-application-btn",
                            ),
                        ),
                        mui.Card(
                            mui.H3("Select LLM for synthetic data generation"),
                            fh.Div(
                                mui.Select(
                                    *[
                                        fh.Option(
                                            serving_endpoint.name,
                                            value=serving_endpoint.name,
                                        )
                                        for serving_endpoint in serving_endpoints
                                    ],
                                    fh.Option(
                                        "--- Select an LLM ---",
                                        value="",
                                        disabled=True,
                                        selected=True,
                                    ),
                                    name="selected_llm",
                                    label="Choose LLM:",
                                    hx_post=handle_llm_selection,
                                    hx_target="#llm-selection-display",
                                    hx_swap="innerHTML",
                                ),
                                fh.Div(id="llm-selection-display", cls="mt-2 text-sm"),
                                cls="space-y-2",
                            ),
                            cls="h-fit",
                        ),
                        cls="flex-1 space-y-4",
                    ),
                    cls="gap-6 items-start",
                ),
            ),
            mui.Container(test_case_table(session), cls=mui.ContainerT.expand),
            cls=mui.ContainerT.expand,
        ),
    )


@rt
def handle_llm_selection(selected_llm_name: str, session):
    """Handle LLM selection from the simple dropdown and return confirmation."""
    session["selected_llm_for_generation"] = selected_llm_name
    return fh.P(f"LLM for generation set to: {selected_llm_name}")


@rt
def evaluations():
    """Show all available evaluations."""
    return mui.Container(
        header,
    )


@rt
def instantiate_table(application_name: str, session):
    """Handle table instantiation with the provided table name."""
    get_tables(application_name)
    session.setdefault("application_name", application_name)
    return mui.H5(
        application_name, cls=mui.TextPresets.bold_lg, id="ai-application-btn"
    )


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
def add_test_case_row_form(session):
    """Returns a new table row with a form to add a test case."""
    next_input_id = get_next_input_id(session)
    next_test_case_id = f"tc_{next_input_id}"

    form_fields = [
        fh.Td(str(next_input_id)),
        fh.Td(str(next_test_case_id)),
        fh.Td(
            mui.Input(name="input", placeholder="Input", required=True, cls="w-full")
        ),
        fh.Td(mui.TextArea(name="output", placeholder="Output", cls="w-full")),
        fh.Td(mui.TextArea(name="notes", placeholder="Notes", cls="w-full")),
        fh.Td(
            mui.Input(
                name="eval_type", placeholder="Eval Type (e.g., good/bad)", cls="w-full"
            )
        ),
        fh.Td(mui.TextArea(name="features", placeholder="Features", cls="w-full")),
        fh.Td(mui.TextArea(name="scenarios", placeholder="Scenarios", cls="w-full")),
        fh.Td(
            mui.TextArea(name="constraints", placeholder="Constraints", cls="w-full")
        ),
        fh.Td(mui.TextArea(name="personas", placeholder="Personas", cls="w-full")),
        fh.Td(
            mui.TextArea(name="assumptions", placeholder="Assumptions", cls="w-full")
        ),
        fh.Td(
            mui.TextArea(
                name="generate_test_case_prompt",
                placeholder="Generate Test Case Prompt",
                cls="w-full",
            )
        ),
        fh.Td(
            mui.TextArea(name="agent_input", placeholder="Agent Input", cls="w-full")
        ),
        fh.Td(
            mui.TextArea(name="agent_output", placeholder="Agent Output", cls="w-full")
        ),
        fh.Td(mui.Button("Save", type="submit", cls=mui.ButtonT.primary)),
    ]
    return fh.Tr(
        *form_fields,
        hx_post="/save_test_case_row",
        hx_target="#test-case-table-container",
        hx_swap="outerHTML",
    )


@rt
def save_test_case_row(
    session,
    input: str,
    output: str | None = None,
    notes: str | None = None,
    eval_type: str | None = None,
    features: str | None = None,
    scenarios: str | None = None,
    constraints: str | None = None,
    personas: str | None = None,
    assumptions: str | None = None,
    generate_test_case_prompt: str | None = None,
    agent_input: str | None = None,
    agent_output: str | None = None,
    # test_case_id is not taken as input, it's auto-generated
):
    """Saves the new test case row data to the session and returns the updated table."""
    test_cases = session.get("test_cases", [])
    next_input_id = get_next_input_id(session)
    next_test_case_id = f"tc_{next_input_id}"

    new_test_case = {
        "input_id": next_input_id,
        "test_case_id": next_test_case_id,
        "input": input,
        "output": output if output is not None else "",
        "notes": notes if notes is not None else "",
        "eval_type": eval_type if eval_type is not None else "pending",
        "features": features if features is not None else "",
        "scenarios": scenarios if scenarios is not None else "",
        "constraints": constraints if constraints is not None else "",
        "personas": personas if personas is not None else "",
        "assumptions": assumptions if assumptions is not None else "",
        "generate_test_case_prompt": generate_test_case_prompt
        if generate_test_case_prompt is not None
        else "",
        "agent_input": agent_input if agent_input is not None else "",
        "agent_output": agent_output if agent_output is not None else "",
    }
    test_cases.append(new_test_case)
    session["test_cases"] = test_cases

    return test_case_table(session)


@rt
def edit_test_case_form(input_id: int, session):
    """Returns an editable form row for the given input_id."""
    test_cases = session.get("test_cases", [])
    test_case_to_edit = None
    for tc in test_cases:
        if tc.get("input_id") == input_id:
            test_case_to_edit = tc
            break

    if not test_case_to_edit:
        return fh.Tr(fh.Td("Test case not found", colspan=len(DISPLAY_COLUMNS) + 1))

    form_fields = [
        fh.Td(str(test_case_to_edit.get("input_id"))),
        fh.Td(str(test_case_to_edit.get("test_case_id"))),
        fh.Td(
            mui.Input(
                name="input",
                value=test_case_to_edit.get("input", ""),
                required=True,
                cls="w-full",
            )
        ),
        fh.Td(
            mui.TextArea(
                name="output", value=test_case_to_edit.get("output", ""), cls="w-full"
            )
        ),
        fh.Td(
            mui.TextArea(
                name="notes", value=test_case_to_edit.get("notes", ""), cls="w-full"
            )
        ),
        fh.Td(
            mui.Input(
                name="eval_type",
                value=test_case_to_edit.get("eval_type", ""),
                cls="w-full",
            )
        ),
        fh.Td(
            mui.TextArea(
                name="features",
                value=test_case_to_edit.get("features", ""),
                cls="w-full",
            )
        ),
        fh.Td(
            mui.TextArea(
                name="scenarios",
                value=test_case_to_edit.get("scenarios", ""),
                cls="w-full",
            )
        ),
        fh.Td(
            mui.TextArea(
                name="constraints",
                value=test_case_to_edit.get("constraints", ""),
                cls="w-full",
            )
        ),
        fh.Td(
            mui.TextArea(
                name="personas",
                value=test_case_to_edit.get("personas", ""),
                cls="w-full",
            )
        ),
        fh.Td(
            mui.TextArea(
                name="assumptions",
                value=test_case_to_edit.get("assumptions", ""),
                cls="w-full",
            )
        ),
        fh.Td(
            mui.TextArea(
                name="generate_test_case_prompt",
                value=test_case_to_edit.get("generate_test_case_prompt", ""),
                cls="w-full",
            )
        ),
        fh.Td(
            mui.TextArea(
                name="agent_input",
                value=test_case_to_edit.get("agent_input", ""),
                cls="w-full",
            )
        ),
        fh.Td(
            mui.TextArea(
                name="agent_output",
                value=test_case_to_edit.get("agent_output", ""),
                cls="w-full",
            )
        ),
        fh.Td(mui.Button("Update", type="submit", cls=mui.ButtonT.primary)),
    ]

    return fh.Tr(
        *form_fields,
        id=f"row-{input_id}",
        hx_post=f"/update_test_case_row/{input_id}",
        hx_target="#test-case-table-container",
        hx_swap="outerHTML",
    )


@rt
def update_test_case_row(
    input_id: int,
    session,
    input: str,
    output: str | None = None,
    notes: str | None = None,
    eval_type: str | None = None,
    features: str | None = None,
    scenarios: str | None = None,
    constraints: str | None = None,
    personas: str | None = None,
    assumptions: str | None = None,
    generate_test_case_prompt: str | None = None,
    agent_input: str | None = None,
    agent_output: str | None = None,
):
    """Updates an existing test case in the session and returns the updated table."""
    test_cases = session.get("test_cases", [])
    updated_test_cases = []
    found = False
    for tc in test_cases:
        if tc.get("input_id") == input_id:
            tc["input"] = input
            # tc["test_case_id"] remains unchanged
            tc["output"] = output if output is not None else tc.get("output", "")
            tc["notes"] = notes if notes is not None else tc.get("notes", "")
            tc["eval_type"] = (
                eval_type if eval_type is not None else tc.get("eval_type", "pending")
            )
            tc["features"] = (
                features if features is not None else tc.get("features", "")
            )
            tc["scenarios"] = (
                scenarios if scenarios is not None else tc.get("scenarios", "")
            )
            tc["constraints"] = (
                constraints if constraints is not None else tc.get("constraints", "")
            )
            tc["personas"] = (
                personas if personas is not None else tc.get("personas", "")
            )
            tc["assumptions"] = (
                assumptions if assumptions is not None else tc.get("assumptions", "")
            )
            tc["generate_test_case_prompt"] = (
                generate_test_case_prompt
                if generate_test_case_prompt is not None
                else tc.get("generate_test_case_prompt", "")
            )
            tc["agent_input"] = (
                agent_input if agent_input is not None else tc.get("agent_input", "")
            )
            tc["agent_output"] = (
                agent_output if agent_output is not None else tc.get("agent_output", "")
            )
            found = True
        updated_test_cases.append(tc)

    if not found:
        pass

    session["test_cases"] = updated_test_cases
    return test_case_table(session)


fh.serve()
