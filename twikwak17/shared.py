"""Shared stuff for twikwak17."""

import os
import time
import json
import subprocess
import multiprocessing
from datetime import datetime, timedelta
from shutil import copyfile

from birch import Birch
from psutil import virtual_memory

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

P1_USER_LIST_FNAME = 'twitter7_user_list.txt.gz'
P1_SORTED_USER_LIST_FNAME = 'twitter7_user_list_sorted.txt.gz'


def t7_user_list_fpath_by_dpath(dpath, sorted=False):
    if sorted:
        return os.path.join(dpath, P1_SORTED_USER_LIST_FNAME)
    return os.path.join(dpath, P1_USER_LIST_FNAME)


P1_TWEET_LIST_FNAME = 'twitter7_tweet_list.txt.gz'
P1_TWEET_LIST_SORTED_FNAME = 'twitter7_tweet_list_sorted.txt.gz'


def twitter7_tweet_list_fpath_by_dpath(dpath, sorted=False):
    if sorted:
        return os.path.join(dpath, P1_TWEET_LIST_SORTED_FNAME)
    return os.path.join(dpath, P1_TWEET_LIST_FNAME)


# --- phase 2 ----

P2_UNAME_2_ID_FNAME = 'kwak10_uname_to_id.txt.gz'
P2_SORTED_UNAME_2_ID_FNAME = 'kwak10_uname_to_id_sorted.txt.gz'
P2_KWAK10_UNAMES_FNAME = 'kwak10_unames.txt.gz'
P2_KWAK10_UNAMES_SORTED_FNAME = 'kwak10_unames_sorted.txt.gz'


def uname2id_fpath_by_dpath(dpath, sorted=False):
    if sorted:
        return os.path.join(dpath, P2_SORTED_UNAME_2_ID_FNAME)
    return os.path.join(dpath, P2_UNAME_2_ID_FNAME)


def kwak10_unames_fpath_by_dpath(dpath, sorted=False):
    if sorted:
        return os.path.join(dpath, P2_KWAK10_UNAMES_SORTED_FNAME)
    return os.path.join(dpath, P2_KWAK10_UNAMES_FNAME)


# --- phase 3 ---

UNAME_INTERSECTION_FNAME = 'uname_intersection.txt.gz'


def uname_intersection_fpath_by_dpath(dpath):
    return os.path.join(dpath, UNAME_INTERSECTION_FNAME)


# --- phase 4 ---

GENDER_MAPPING_FNAME = 'username_to_gender.txt.gz'


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


# === Other ===

# see full documentation for GNU sort in
# http://www.gnu.org/software/coreutils/manual/html_node/sort-invocation.html
# https://unix.stackexchange.com/questions/120096/how-to-sort-big-files
# Gz-sort also looks interesting, but I chose to not use it:
# http://kmkeen.com/gz-sort/
UNGZIP_CMD_TMPLT = "gzip -dc {input_fpath}"
GNU_SORT_CMD_TMPLT = (
    'sort -u -S {mem_bytes}b --parallel={ncores} '
    '-T ~/ --compress-program=gzip '
)
# GZIP_CMiD_TMPLT = "gzip > {output_fpath}"

BYTES_IN_MB = 1000000
AVAIL_MEM_TO_LEAVE_BYTES = 500 * BYTES_IN_MB


def sort_username_file(input_fpath, output_fpath=None):
    """Sorts the given username file accroding to native byte ordering.

    Parameters
    ----------
    input_fpath : str
        The full path to the input file.
    output_fpath : str, optional
        The full path to the output file. If not given, the string '_sorted'
        is appended to the input file path (but before file extension).

    Returns
    -------
    output_fpath : str
        The full path to the output file.
    """
    # construct parameters
    if output_fpath is None:
        input_fpath_no_ext, ext = input_fpath.split(os.extsep, 1)
        output_fpath = input_fpath_no_ext + '_sorted.' + ext
    avail_memory_bytes = virtual_memory().available
    memory_to_use_bytes = avail_memory_bytes - AVAIL_MEM_TO_LEAVE_BYTES
    qprint("Starting to sort username file!")
    qprint(f"Source: {input_fpath}")
    qprint(f"Target: {output_fpath}")
    qprint(f"Memory to use (in MB): {memory_to_use_bytes / BYTES_IN_MB}")
    qprint(f"Cores to use: {multiprocessing.cpu_count()}")

    # construct running environment
    sort_env = os.environ.copy()
    sort_env['LC_ALL'] = 'C'

    # construct commands
    ungzip_cmd = UNGZIP_CMD_TMPLT.format(input_fpath=input_fpath)
    dd_cmd = 'dd conv=lcase'
    sort_cmd = GNU_SORT_CMD_TMPLT.format(
        mem_bytes=memory_to_use_bytes,
        ncores=multiprocessing.cpu_count(),
    )
    # gzip_cmd = GZIP_CMD_TMPLT.format(output_fpath=output_fpath)
    gzip_cmd = 'gzip'
    qprint(
        f"Commands to run: \n{ungzip_cmd}\n{dd_cmd}\n{sort_cmd}\n{gzip_cmd}")
    qprint(f"Equivalent to: {ungzip_cmd} | 2>/dev/null {dd_cmd} | "
           "LC_ALL=C {sort_cmd} | {gzip_cmd} > {output_fpath}")

    # construct processes
    with open(output_fpath, 'wt+') as output_f:
        ungzip_process = subprocess.Popen(
            ungzip_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=sort_env)
        dd_process = subprocess.Popen(
            dd_cmd.split(), stdin=ungzip_process.stdout,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=sort_env)
        sort_process = subprocess.Popen(
            sort_cmd.split(), stdin=dd_process.stdout,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=sort_env)
        gzip_process = subprocess.Popen(
            gzip_cmd.split(), stdin=sort_process.stdout, stdout=output_f,
            stderr=subprocess.PIPE, env=sort_env)

    # run processes
    qprint('ungzip stderr:')
    qprint(ungzip_process.stderr.read())
    ungzip_process.stdout.close()
    qprint('dd stderr:')
    qprint(dd_process.stderr.read())
    dd_process.stdout.close()
    qprint('sort stderr:')
    qprint(sort_process.stderr.read())
    sort_process.stdout.close()
    qprint('gzip stderr:')
    qprint(gzip_process.stderr.read())
    output = gzip_process.communicate()[0]
    qprint(f'Output:\n {output}')
    # qprint('stderr:')
    # qprint(f'Results:\n {result.stdout}')
    # qprint(f'Errors:\n {result.stderr}')
    return output_fpath
