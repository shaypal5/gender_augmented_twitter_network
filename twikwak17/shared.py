"""Shared stuff for twikwak17."""

from birch import Birch

TWIK_CFG = Birch('twikwak17')

DEF_FNAME_PATTERN = 'tweets2009-{:02d}.txt.gz'
DEF_FNAMES = [
    DEF_FNAME_PATTERN.format(month)
    for month in range(6, 13)
]


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
