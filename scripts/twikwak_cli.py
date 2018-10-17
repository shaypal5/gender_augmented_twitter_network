"""Defines a command-line interface for twikwak17."""

import click

import twikwak17


@click.group()
def cli():
    """Command-line interface for the twikwak17 package."""
    pass


SILENT = click.option(
    '-s', '--silent/--verbose', default=False,
    help="Don't print any messages to screen. Defaults to verbose."
)


SAMPLE_DOC = "Generate a sample of the twitter7 dataset."


@cli.command(help=SAMPLE_DOC + (
    "\n\n Arguments:\n\n SIZE The the sample size, in tweets."
    "\n\n SOURCE The path to the source twitter7 file."
    "\n\n TARGET The path to the target sample file."
))
@click.argument("size", type=int, nargs=1)
@click.argument("source", type=str, nargs=1)
@click.argument("target", type=str, nargs=1)
@SILENT
def sample(size, source, target, silent):
    """{}""".format(SAMPLE_DOC)
    twikwak17.sample_twitter7(size, source, target, silent)
