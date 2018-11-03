"""Defines a command-line interface for twikwak17."""

import click

import twikwak17


@click.group()
def cli():
    """Command-line interface for the twikwak17 package."""
    pass


SILENT = click.option(
    '-q', '--quiet/--verbose', default=False,
    help="Don't print any messages to screen. Defaults to verbose."
)


SAMPLE_FILE_DOC = "Generate a sample of a twitter7 file."


@cli.command(help=SAMPLE_FILE_DOC + (
    "\n\n Arguments:\n\n SIZE The the sample size, in tweets."
))
@click.argument("size", type=int, nargs=1)
@click.option(
    '-s', '--source', type=str,
    help="The path to the source twitter7 file."
)
@click.option(
    '-t', '--target', type=str,
    help="The path to the target sample file."
)
@SILENT
def sample_file(size, source, target, quiet):
    """{}""".format(SAMPLE_FILE_DOC)
    twikwak17.sample_twitter7_file(size, source, target, quiet)


SAMPLE_DOC = "Generate a sample of the twitter7 dataset."


@cli.command(help=SAMPLE_DOC + (
    "\n\n Arguments:\n\n SIZE The the sample size, in tweets."
))
@click.argument("size", type=int, nargs=1)
@click.option(
    '-s', '--source', type=str,
    help="The path to the source twitter7 folder."
)
@click.option(
    '-t', '--target', type=str,
    help="The path to the target sample folder."
)
@SILENT
def sample_folder(size, source, target, quiet):
    """{}""".format(SAMPLE_DOC)
    twikwak17.sample_twitter7_folder(size, source, target, quiet)
