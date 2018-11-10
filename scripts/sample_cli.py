"""Sampling related CLI commands for the twikwak17 package."""

import click

import twikwak17

from .shared_options import (
    SILENT,
    TPATH,
)


SAMPLE_FILE_DOC = "Generate a sample of a twitter7 file."


@click.command(help=SAMPLE_FILE_DOC + (
    "\n\n Arguments:\n\n SIZE The the sample size, in tweets."
))
@click.argument("size", type=int, nargs=1)
@click.option(
    '-f', '--fpath', type=str,
    help="The path to the twitter7 file to sample."
)
@click.option(
    '-o', '--output', type=str,
    help="The path to the output sample file."
)
@SILENT
def sample_file(size, source, target, quiet):
    """{}""".format(SAMPLE_FILE_DOC)
    twikwak17.shared.set_print_quiet(quiet)
    twikwak17.sample_twitter7_file(size, source, target, quiet)


SAMPLE_DOC = "Generate a sample of the twitter7 dataset."


@click.command(help=SAMPLE_DOC + (
    "\n\n Arguments:\n\n SIZE The the sample size, in tweets."
))
@click.argument("size", type=int, nargs=1)
@TPATH
@click.option(
    '-o', '--output', type=str,
    help=("The path to the output sample folder. If not given, an appropriate"
          " sub-folder is created inside the twitter7 dataset folder.")
)
@SILENT
def sample_folder(size, tpath, output, quiet):
    """{}""".format(SAMPLE_DOC)
    twikwak17.shared.set_print_quiet(quiet)
    twikwak17.sample_twitter7_folder(size, tpath, output)
