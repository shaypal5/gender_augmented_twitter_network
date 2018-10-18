"""Phase 1 of the twikwak17 dataset generation process."""

import gzip
import json
from psutil import virtual_memory
from time import time

from ezenum import StringEnum
from sortedcontainers import SortedDict

from twikwak17.shared import (
   qprint,
)


LINETYPE = StringEnum(['Time', 'User', 'Content', 'Other'])


def interpret_line(decoded_line):
    try:
        char1 = decoded_line[1]
        if char1 == '\t':
            char0 = decoded_line[0]
            if char0 == 'T':
                return LINETYPE.Time, decoded_line[2:-1]
            elif char0 == 'U':
                return LINETYPE.User, decoded_line[21:-1]
            elif char0 == 'W':
                return LINETYPE.Content, decoded_line[2:-1]
            else:
                return LINETYPE.Other, decoded_line[2:-1]
        else:
            return LINETYPE.Other, ''
    except IndexError:
        return LINETYPE.Other, ''


def dump_usr_2_twits_str_to_file(usr_2_twits_str, tweets_fpath, usr_fpath):
    with gzip.open(tweets_fpath, 'wt') as f:
        for user, tweets in usr_2_twits_str.items():
            f.write('{} {}\n'.format(user, tweets))
    with open(usr_fpath, 'w') as f:
        json.dump(list(usr_2_twits_str.keys()), f)


NO_CONTENT_STR = 'No Post Title'
MONITOR_LINE_FREQ_DEF = 1000000
BYTES_IN_MB = 1000000
MIN_AVAIL_MEM_MB_DEF = 500
REPORT_TEMPLATE = (
    '{:.2f} min running | {} lines processed | ~ {} tweets processed |'
    ' {} tpm | {} files written | {} available memory'
)


def merge_user_tweets_in_file(
        fpath, monitor_line_freq=None, min_mem_mb=None):
    """Splits a raw twitter7 tweets file into user-merged subset files.

    The user order in each resulting file is lexicographical.

    Parameters
    ----------
    fpath : str
        The full qualified path to the twitter7 file to process.
    monitor_line_freq : int, optional
        Monitoring messages will be printed every this number of lines.
    min_mem_mb : int, optional
        The currently held tweets will be dumped to file when this number
        of megabytes remain in available memory.
    """
    if monitor_line_freq is None:
        monitor_line_freq = MONITOR_LINE_FREQ_DEF
    if min_mem_mb is None:
        min_mem_mb = MIN_AVAIL_MEM_MB_DEF
    min_mem_bytes = min_mem_mb * BYTES_IN_MB
    qprint((
        "Merging tweets by user in {}."
        "\nMonitor line frequency is {} and min allowed memory (MB) is {}."
    ).format(fpath, monitor_line_freq, min_mem_mb))
    most_recent_user = None
    # starting_available_mem = virtual_memory().available
    start_time = time()
    usr_2_twits_str = SortedDict()
    files_written = 0

    def _report():
        av_mem = virtual_memory().available
        seconds_running = time() - start_time
        report = REPORT_TEMPLATE.format(
            seconds_running / 60,
            i,
            i / 4,
            (i / 4) / (seconds_running / 60),
            files_written,
            av_mem,
        )
        qprint(report, end='\r')

    def _dump_file():
        nonlocal usr_2_twits_str, files_written
        dump_fpath = '{}_p1dump_{}.txt.gz'.format(
            fpath[:fpath.find('.')], files_written)
        usr_fpath = '{}_p1usr_{}.json'.format(
            fpath[:fpath.find('.')], files_written)
        dump_usr_2_twits_str_to_file(
            usr_2_twits_str=usr_2_twits_str,
            tweets_fpath=dump_fpath,
            usr_fpath=usr_fpath,
        )
        files_written += 1
        usr_2_twits_str = SortedDict()
        # qprint('Tweets dumped for the {}-th time into {}'.format(
        #     files_written, dump_fpath))
        # qprint('Tweets dumped for the {}-th time into {}'.format(
        #     files_written, dump_fpath))

    with gzip.open(fpath, 'rt') as textf:
        for i, line in enumerate(textf):
            try:
                ltype, lcontent = interpret_line(line)
                if ltype == LINETYPE.User:
                    most_recent_user = lcontent
                elif ltype == LINETYPE.Content and lcontent != NO_CONTENT_STR:
                    usr_2_twits_str[most_recent_user] = usr_2_twits_str.get(
                        most_recent_user, '') + ' ' + lcontent
            except Exception as e:
                qprint(line)
                qprint(interpret_line(line))
                raise e
            if i % monitor_line_freq == 0:
                _report()
                av_mem = virtual_memory().available
                if av_mem < min_mem_bytes:
                    _dump_file()
    if len(usr_2_twits_str) > 0:
        _dump_file()
        _report()
