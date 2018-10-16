"""Phase 1 of the twikwak17 dataset generation process."""

import gzip
from psutil import virtual_memory
from time import time

from ezenum import StringEnum
from sortedcontainers import SortedDict

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


def dump_usr_2_twits_str_to_file(usr_2_twits_str):


NO_CONTENT_STR = 'No Post Title'
MONITOR_LINE_FREQ_DEF = 1000000
MIN_AVAIL_MEM_BYTES_DEF = 500 * 1000000
REPORT_TEMPLATE = (
    '{:.2f} min running | {} lines processed | ~ {} tweets processed |'
    ' {} tpm | {} available memory'
)


def merge_user_tweets_in_file(fpath, monitor_line_freq=None, min_mem_mb=None):
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
        min_mem_mb = MIN_AVAIL_MEM_BYTES_DEF
    most_recent_user = None
    # starting_available_mem = virtual_memory().available
    start_time = time()
    usr_2_twits_str = SortedDict()
    with gzip.open(fpath, 'r') as textf:
        for i, line in enumerate(textf):
            try:
                decoded_line = line.decode('utf-8')
                ltype, lcontent = interpret_line(decoded_line)
                if ltype == LINETYPE.User:
                    most_recent_user = lcontent
                elif ltype == LINETYPE.Content and lcontent != NO_CONTENT_STR:
                    usr_2_twits_str[most_recent_user] = usr_2_twits_str.get(
                        most_recent_user, '') + ' ' + lcontent
            except Exception as e:
                print(decoded_line)
                print(interpret_line(decoded_line))
                raise e
            if i % monitor_line_freq == 0:
                av_mem = virtual_memory().available
                seconds_running = time() - start_time
                report = REPORT_TEMPLATE.format(
                    seconds_running / 60,
                    i,
                    i / 4,
                    (i / 4) / (seconds_running / 60),
                    av_mem,
                )
                print(report, end='\r')
                if av_mem < min_mem_mb:
                    return
