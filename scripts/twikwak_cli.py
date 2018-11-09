"""Defines a command-line interface for twikwak17."""

import click

# import twikwak17

from .sample_cli import (
    sample_file,
    sample_folder,
)
from .shared_options import (
    SILENT,
    TPATH,
)


@click.group()
def cli():
    """Command-line interface for the twikwak17 package."""
    pass


cli.add_command(sample_file)
cli.add_command(sample_folder)


RUN_PIPELINE_DOC = "Runs the entire twikwak17 generation pipeline."


@cli.command(help=RUN_PIPELINE_DOC + (
    "\n\n Can be configured by populating ~/.config/twikwak17/cfg.json."
))
@TPATH
@click.option(
    '-k', '--kpath', type=str,
    help=("The path to the kwak10www dataset folder. If not given, the value "
          "keyed to 'kwak10_dpath' is looked up in the twikwak17 "
          "configuration file.")
)
@click.option(
    '-o', '--output', type=str,
    help=("The path to a designated output folder. If not given, the value "
          "keyed to 'output_dpath' is looked up in the twikwak17 "
          "configuration file.")
)
@SILENT
def run_pipeline(tpath, kpath, output, quiet):
    """{}""".format(RUN_PIPELINE_DOC)
    print("{} {} {} {}".format(tpath, kpath, output, quiet))
