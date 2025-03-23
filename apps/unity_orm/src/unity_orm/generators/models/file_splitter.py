"""File splitting utilities for model generation."""

import os


def split_models_to_files(models_content: str, output_dir: str) -> None:
    """Split the generated models content into individual files.

    Args:
        models_content: The content of the models file.
        output_dir: The directory to write the files to.
    """
    lines = models_content.splitlines()
    import_lines = []

    i = 0
    while i < len(lines) and not lines[i].startswith("class "):
        import_lines.append(lines[i])
        i += 1

    with open(os.path.join(output_dir, "__init__.py"), "w") as f:
        f.write("# Auto-generated SQLAlchemy models\n\n")

        class_names = []
        for line in lines:
            if line.startswith("class "):
                class_name = line.split("(")[0].replace("class ", "").strip()
                class_names.append(class_name)

        for class_name in class_names:
            snake_name = camel_to_snake(class_name)
            f.write(f"from .{snake_name} import {class_name}\n")

        f.write("\n__all__ = [\n")
        for class_name in class_names:
            f.write(f"    '{class_name}',\n")
        f.write("]\n")

    class_start_indices = [
        i for i, line in enumerate(lines) if line.startswith("class ")
    ]

    import_content = "\n".join(import_lines)
    import_content = import_content.replace(
        "from sqlalchemy.ext.declarative import declarative_base",
        "from unity_orm.model_base import ManagedTable",
    )
    import_content = import_content.replace(
        "from sqlalchemy.orm import declarative_base",
        "from unity_orm.model_base import ManagedTable",
    )
    import_content = import_content.replace("Base = declarative_base()", "")

    for i, start_idx in enumerate(class_start_indices):
        end_idx = (
            class_start_indices[i + 1]
            if i + 1 < len(class_start_indices)
            else len(lines)
        )

        class_lines = lines[start_idx:end_idx]
        class_content = "\n".join(class_lines)

        class_name = class_lines[0].split("(")[0].replace("class ", "").strip()
        snake_name = camel_to_snake(class_name)

        class_content = class_content.replace("(Base):", "(ManagedTable):")

        file_path = os.path.join(output_dir, f"{snake_name}.py")
        with open(file_path, "w") as f:
            f.write(import_content + "\n\n")
            f.write(class_content)

        print(f"Created model file: {file_path}")


def camel_to_snake(name: str) -> str:
    """Convert a CamelCase name to snake_case.

    Args:
        name: The CamelCase name.

    Returns:
        The snake_case name.
    """
    snake_name = ""
    for char in name:
        if char.isupper() and snake_name:
            snake_name += "_" + char.lower()
        else:
            snake_name += char.lower()
    return snake_name
