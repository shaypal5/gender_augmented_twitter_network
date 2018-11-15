"""Shared stuff for twikwak17."""

import os
from datetime import datetime, timedelta

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


DEF_TWITTER7_FNAME_PATTERN = 'tweets2009-[\w\d_]+.txt.gz'
DEF_TWITTER7_FNAME_TEMPLATE = 'tweets2009-{:02d}.txt.gz'
DEF_TWITTER7_FNAMES = [
    DEF_TWITTER7_FNAME_TEMPLATE.format(month)
    for month in range(6, 13)
]


def twitter7_dpath():
    return TWIK_CFG.get(CfgKey.TWITTER7_DPATH, None)


def kwak10_dpath():
    return TWIK_CFG.get(CfgKey.KWAK10_DPATH, None)


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


# --- phase 1 ----

USER_LIST_FNAME = 'twitter7_user_list.txt.gz'


def user_list_fpath_by_dpath(dpath):
    return os.path.join(dpath, USER_LIST_FNAME)


TWEET_LIST_FNAME = 'twitter7_tweet_list.txt.gz'


def tweet_list_fpath_by_dpath(dpath):
    return os.path.join(dpath, TWEET_LIST_FNAME)


# --- phase 2 ----

UNAME_2_ID_FNAME = 'kwak10_uname_to_id.txt.gz'
KWAK10_UNAMES_FNAME = 'kwak10_unames.txt.gz'


def uname2id_fpath_by_kpath(kpath):
    return os.path.join(kpath, UNAME_2_ID_FNAME)


def kwak10_unames_fpath_by_kpath(kpath):
    return os.path.join(kpath, KWAK10_UNAMES_FNAME)


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


def seconds_to_duration_str(duration_in_seconds):
    sec = timedelta(seconds=duration_in_seconds)
    d = datetime(1, 1, 1) + sec
    return "{} days and {}:{}:{}".format(d.day-1, d.hour, d.minute, d.second)
