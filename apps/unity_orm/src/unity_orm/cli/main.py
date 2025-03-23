#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Command-line interface for Unity ORM."""

import sys
import traceback

import click

# Add package commands
try:
    from unity_orm.cli.commands import (
        interactive_commands,
        model_commands,
    )
except ImportError:
    # During development, the commands modules might not be available yet
    model_commands = None
    interactive_commands = None


@click.group()
def cli():
    """Command-line interface for Unity ORM."""
    return cli


def main():
    """Run the CLI."""
    try:
        # Run the CLI
        cli()
    except Exception as e:
        print(f"Error: {str(e)}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
