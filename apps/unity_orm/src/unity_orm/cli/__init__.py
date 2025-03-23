"""CLI package for Unity ORM."""

# Import for backward compatibility
# Register commands at import time
from unity_orm.cli.commands import interactive_commands, model_commands
from unity_orm.cli.main import cli, main

# Register commands
model_commands.register_commands(cli)
interactive_commands.register_commands(cli)

__all__ = ["cli", "main"]
