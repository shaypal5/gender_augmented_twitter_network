"""Shared stuff for twikwak17."""

import os
import time
import json
from datetime import datetime, timedelta
from shutil import copyfile

from birch import Birch

from .exceptions import TwikwakConfigurationError


# === general ===

DONE_MARKER = 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'


# === configuration ===

TWIK_CFG = Birch('twikwak17')
TWIK_CFG_DPATH = os.path.expanduser('~/.config/twikwak17/')
TWIK_CFG_FPATH = os.path.join(TWIK_CFG_DPATH, 'cfg.json')


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


# --- session saves ---

TWIK_CFG_SESSION_DPATH = os.path.join(TWIK_CFG_DPATH, 'sessions')
os.makedirs(TWIK_CFG_SESSION_DPATH, exist_ok=True)


def time_to_nice_time_str(time_in_sec):
        gt = time.gmtime(time_in_sec)
        return (f'{gt.tm_year}_{gt.tm_mon:02}_{gt.tm_mday:02}_'
                f'{gt.tm_hour:02}:{gt.tm_min:02}:{gt.tm_sec:02}')


class Session(object):
    """A session of running the twikwak17 generation pipeline.

    Parameters
    ----------
    start_time : float
        The original start time of this session, in seconds since the epoch.
        As returned by time.time().
    kwargs : dict of str to str/float/int
        The original kwargs the session was started with.
    phases : dict, optional
        A dict of nested dicts mapping the save parameters of all subphases.
        If not given, initialized empty, to signify a new session.
    last_completed_subphase : str, optional
        The last subphase completed by the session; if not given, initialized
        to None, to signify a new session.
    """

    def __init__(
            self, start_time, kwargs, phases=None,
            last_completed_subphase=None):
        self.start_time = start_time
        self.kwargs = kwargs
        self.phases = {}
        self.last_completed_phase = None
        self.last_completed_subphase = None
        self.save_time = None

    def fpath(self):
        """Returns the file path for saves of this session."""
        tstr = time_to_nice_time_str(self.start_time)
        return os.path.join(TWIK_CFG_DPATH, f'session_{tstr}.json')

    def save(self, phase, subphase, subphase_params):
        """Saves the current state of this session to disk.

        Parameters
        ----------
        phase : int
            The last completed phase.
        subphase : str
            The last completed subphase.
        subphase_params : dict
            Parameters describing the state of the last_completed subphase.
        """
        self.save_time = time.time()
        self.last_completed_phase = phase
        self.last_completed_subphase = subphase
        json.dump(self.__dict__, self.fpath())

    @staticmethod
    def load(session_fpath):
        with open(session_fpath, 'rt') as f:
            attr_dict = json.load(f)
        return Session(**attr_dict)


# --- shared paths ---


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


def phase_output_report_fpath(phase, dpath):
    return os.path.join(
        dpath,
        f'phase_{phase}_output_report.txt',
    )


# --- phase 1 ----

USER_LIST_FNAME = 'twitter7_user_list.txt.gz'


def t7_user_list_fpath_by_dpath(dpath):
    return os.path.join(dpath, USER_LIST_FNAME)


TWEET_LIST_FNAME = 'twitter7_tweet_list.txt.gz'


def twitter7_tweet_list_fpath_by_dpath(dpath):
    return os.path.join(dpath, TWEET_LIST_FNAME)


# --- phase 2 ----

UNAME_2_ID_FNAME = 'kwak10_uname_to_id.txt.gz'
KWAK10_UNAMES_FNAME = 'kwak10_unames.txt.gz'


def uname2id_fpath_by_dpath(dpath):
    return os.path.join(dpath, UNAME_2_ID_FNAME)


def kwak10_unames_fpath_by_dpath(dpath):
    return os.path.join(dpath, KWAK10_UNAMES_FNAME)


# --- phase 3 ---

UNAME_INTERSECTION_FNAME = 'uname_intersection.txt.gz'


def uname_intersection_fpath_by_dpath(dpath):
    return os.path.join(dpath, UNAME_INTERSECTION_FNAME)


# --- phase 4 ---

GENDER_MAPPING_FNAME = 'username_to_gender_txt.gz'


def uname_to_gender_map_fpath_by_dpath(dpath):
    return os.path.join(dpath, GENDER_MAPPING_FNAME)


# === printing ===

QUIET = False
SESSION_LOG_FPATH = None
OUTPUT_REPORT_F_HANDLE = None


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


def set_output_report_file_handle(f_handle):
    """Sets the given file handle for as the output report handle.

    Once set, qprint() calls are also written to the output report.

    Parameters
    ----------
    f_handle : file
        If None is given, writing to the output report is ceased.
    """
    global OUTPUT_REPORT_F_HANDLE
    OUTPUT_REPORT_F_HANDLE = f_handle


if CfgKey.QUIET in TWIK_CFG:
    set_print_quiet(TWIK_CFG[CfgKey.QUIET])


def qprint(*args, **kwargs):
    """A print function that can be quieted."""
    if not QUIET:
        print(*args, **kwargs)
    if SESSION_LOG_FPATH is not None:
        with open(SESSION_LOG_FPATH, 'ta+') as f:
            print(*args, file=f)
    if OUTPUT_REPORT_F_HANDLE is not None:
        print(*args, file=OUTPUT_REPORT_F_HANDLE)


def seconds_to_duration_str(duration_in_seconds):
    sec = timedelta(seconds=duration_in_seconds)
    d = datetime(1, 1, 1) + sec
    return "{} days and {}:{}:{}".format(d.day-1, d.hour, d.minute, d.second)


def create_timestamped_report_file_copy(report_fpath):
    fpath_no_ext, ext = os.path.splitext(report_fpath)
    time_str = time_to_nice_time_str(time.time())
    copy_fpath = fpath_no_ext + '_' + time_str + ext
    copyfile(report_fpath, copy_fpath)
