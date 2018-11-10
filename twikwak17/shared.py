"""Shared stuff for twikwak17."""

import os

from birch import Birch

from .exceptions import TwikwakConfigurationError


# === configuration ===

TWIK_CFG = Birch('twikwak17')
TWIK_CFG_FPATH = '~/.config/twikwak17/cfg.json'


class CfgKey(object):
    QUIET = 'quiet'
    SAMPLE_OUTPUT_DPATH = 'sample_output_dpath'
    TWITTER7_DPATH = 'twitter7_dpath'
    KWAK10_DPATH = 'kwak10_dpath'
    OUTPUT_DPATH = 'output_dpath'


def error_raising_cfg_val_get(input_val, cfg_key):
    if input_val is not None:
        return input_val
    try:
        return TWIK_CFG[cfg_key]
    except KeyError:
        raise TwikwakConfigurationError(cfg_key, TWIK_CFG_FPATH)


DEF_FNAME_PATTERN = 'tweets2009-[\w\d_]+.txt.gz'
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


def phase_output_dname(phase_ix):
    return 'phase_{}_output'.format(phase_ix)


def phase_output_dpath(phase_ix, output_dpath):
    dpath = os.path.join(
        output_dpath,
        phase_output_dname(phase_ix),
    )
    os.makedirs(dpath, exist_ok=True)
    return dpath


USER_LIST_FNAME = 'twitter7_user_list.json'


def user_list_fpath_by_dpath(dpath):
    return os.path.join(dpath, USER_LIST_FNAME)


# === printing ===

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


if CfgKey.QUIET in TWIK_CFG:
    set_print_quiet(TWIK_CFG[CfgKey.QUIET])


def qprint(*args, **kwargs):
    """A print function that can be quieted."""
    if not QUIET:
        print(*args, **kwargs)
