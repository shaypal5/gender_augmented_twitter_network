"""Shared CLI options for the twikwak17 package."""

import click


SILENT = click.option(
    '-q', '--quiet/--verbose', default=False,
    help=("Prevents printing of messages to screen. If not given, the value "
          "keyed to 'quiet' is looked up in the twikwak17 configuration file. "
          "Otherwise, defaults to verbose.")
)


TPATH = click.option(
    '-t', '--tpath', type=str,
    help=("The path to the twitter7 dataset folder. If not given, the value "
          "keyed to 'twitter7_dpath' is looked up in the twikwak17 "
          "configuration file.")
)
