"""Shared stuff for twikwak17."""

import os

from birch import Birch


# === configuration ===

TWIK_CFG = Birch('twikwak17')


class CfgKey(object):
    QUIET = 'quiet'
    SAMPLE_OUTPUT_DPATH = 'sample_output_dpath'
    TWITTER7_DPATH = 'twitter7_dpath'
    KWAK10_DPATH = 'kwak10_dpath'
    OUTPUT_DPATH = 'output_dpath'


DEF_FNAME_PATTERN = 'tweets2009-\d+.txt.gz'
DEF_FNAME_TEMPLATE = 'tweets2009-{:02d}.txt.gz'
DEF_FNAMES = [
    DEF_FNAME_TEMPLATE.format(month)
    for month in range(6, 13)
]


def twitter7_dpath():
    return TWIK_CFG.get(CfgKey.TWITTER7_DPATH, None)


DEF_SAMPLE_DNAME_TEMPLATE = 'sample_files'


def sample_output_dpath_by_twitter7_dpath(source_dpath, sample_size):
    if source_dpath:
        fname = '{}_sized_sample_files'.format(sample_size)
        return os.path.join(source_dpath, fname)
    return None


def configured_sample_output_dpath():
    return sample_output_dpath_by_twitter7_dpath(twitter7_dpath())


def phase1_output_dpath(source_dpath):
    return source_dpath


# === pringting ===

QUIET = False


def set_print_quiet(set_val):
    """Sets message printing off or on. Printing is on be default.

    Parameters
    ----------
    set_val : bool
        If True, message printing is turned off. Otherwise, it is turned on.
    """
    global QUIET
    if set_val is not None:
        QUIET = set_val


def qprint(*args, **kwargs):
    """A print function that can be quieted."""
    if not QUIET:
        print(*args, **kwargs)
