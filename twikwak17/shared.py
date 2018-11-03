"""Shared stuff for twikwak17."""

import os

from birch import Birch


# === configuration ===

TWIK_CFG = Birch('twikwak17')

DEF_FNAME_PATTERN = 'tweets2009-\d+.txt.gz'
DEF_FNAME_TEMPLATE = 'tweets2009-{:02d}.txt.gz'
DEF_FNAMES = [
    DEF_FNAME_TEMPLATE.format(month)
    for month in range(6, 13)
]


def default_source_dpath():
    return TWIK_CFG.get('source_dir', None)


DEF_SAMPLE_DNAME_TEMPLATE = 'sample_files'


def sample_dpath_by_source_dpath(source_dpath, sample_size):
    if source_dpath:
        fname = '{}_sized_sample_files'.format(sample_size)
        return os.path.join(source_dpath, DEF_SAMPLE_DNAME)
    return None


def default_sample_dpath():
    return sample_dpath_by_source_dpath(default_source_dpath())


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
    QUIET = set_val


def qprint(*args, **kwargs):
    """A print function that can be quieted."""
    if not QUIET:
        print(*args, **kwargs)
