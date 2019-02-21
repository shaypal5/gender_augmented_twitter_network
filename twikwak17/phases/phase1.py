"""Phase 1 of the twikwak17 dataset generation process."""

import os
import re
import gzip
import time
import gc
from psutil import virtual_memory
from contextlib import ExitStack

from ezenum import StringEnum
from sortedcontainers import SortedDict

from twikwak17.shared import (
    DONE_MARKER,
    qprint,
    DEF_TWITTER7_FNAME_PATTERN,
    twitter7_dpath,
    t7_user_list_fpath_by_dpath,
    twitter7_tweet_list_fpath_by_dpath,
    seconds_to_duration_str,
    sort_username_file,
    phase_output_report_fpath,
    set_output_report_file_handle,
    create_timestamped_report_file_copy,
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
    with gzip.open(tweets_fpath, 'wt+') as tweets_f:
        with gzip.open(usr_fpath, 'wt+') as usr_f:
            for user, tweets in usr_2_twits_str.items():
                tweets_f.write('{} {}\n'.format(user, tweets))
                usr_f.write('{}\n'.format(user))


NO_CONTENT_STR = 'No Post Title'
MONITOR_LINE_FREQ_DEF = 1000000
BYTES_IN_MB = 1000000
MIN_AVAIL_MEM_MB_DEF = 500
REPORT_TEMPLATE = (
    '{:.2f} min running | {:,} lines processed | ~ {:,} tweets processed |'
    ' {:,} tpm | {} files written | {:,.2f} available memory [MB]'
)
DUMP_FNAME_MARKER = 'p1dump'
USR_FNAME_MARKER = 'p1usr'
MIL = 1000000


def order_tweets_by_user_in_file(
        fpath, output_dpath, monitor_line_freq=None, min_mem_mb=None):
    """Splits a raw twitter7 tweets file into user-merged subset files.

    The user order in each resulting file is lexicographical.

    Parameters
    ----------
    fpath : str
        The full qualified path to the twitter7 file to process.
    output_dpath : str
        The path to the designated output folder.
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
        "\n\nMerging tweets by user in {}. "
        "\nMonitor line frequency is {} and min allowed memory (MB) is {}."
    ).format(fpath, monitor_line_freq, min_mem_mb))
    most_recent_user = None
    # starting_available_mem = virtual_memory().available
    start_time = time.time()
    usr_2_twits_str = SortedDict()
    files_written = 0

    def _report():
        av_mem = virtual_memory().available
        seconds_running = time.time() - start_time
        report = REPORT_TEMPLATE.format(
            seconds_running / 60,
            i,
            i / 4,
            (i / 4) / (seconds_running / 60),
            files_written,
            av_mem/MIL,
        )
        qprint(report, end='\r')

    def _mem_report():
        av_mem = virtual_memory().available
        qprint(
            f"Avail. memory: {av_mem/MIL:,} |"
            f" Min mem: {min_mem_bytes/MIL:,}\n")

    def _dump_file(usr_2_twits_str, files_written):
        _mem_report()
        fname = os.path.split(fpath)[1]
        fname = fname[:fname.find('.')]
        dump_fpath = '{}/{}_{}_{}.txt.gz'.format(
            output_dpath, fname, DUMP_FNAME_MARKER, files_written)
        usr_fpath = '{}/{}_{}_{}.txt.gz'.format(
            output_dpath, fname, USR_FNAME_MARKER, files_written)
        dump_usr_2_twits_str_to_file(
            usr_2_twits_str=usr_2_twits_str,
            tweets_fpath=dump_fpath,
            usr_fpath=usr_fpath,
        )
        qprint('\nTweets dumped for the {}-th time into {}'.format(
            files_written + 1, dump_fpath))
        _mem_report()

    _mem_report()
    with gzip.open(fpath, 'rt') as textf:
        for i, line in enumerate(textf):
            try:
                ltype, lcontent = interpret_line(line)
                if ltype == LINETYPE.User:
                    most_recent_user = lcontent.lower()
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
                    _dump_file(usr_2_twits_str, files_written)
                    files_written += 1
                    usr_2_twits_str = None
                    del usr_2_twits_str
                    # try to release memory explixitly
                    gc.collect()
                    usr_2_twits_str = SortedDict()
                    av_mem = virtual_memory().available
    if len(usr_2_twits_str) > 0:
        _dump_file(usr_2_twits_str, files_written)
        files_written += 1
        usr_2_twits_str = None
        del usr_2_twits_str
        # try to release memory explixitly
        gc.collect()
        av_mem = virtual_memory().available
        qprint(
            f"Avail. memory: {av_mem} | Min mem: {min_mem_bytes}")
        _report()


USR_FNAME_RGX = '[\w\d_\-]+{}[\w\d_\.]+'.format(USR_FNAME_MARKER)


def merge_user_files(dpath):
    qprint("Starting to merge all twitter7 user lists in {}".format(dpath))
    filepaths = [
        os.path.join(dpath, fname) for fname in os.listdir(dpath)
        if re.match(pattern=USR_FNAME_RGX, string=fname)
    ]
    qprint("Found {} files to merge.".format(len(filepaths)))
    output_fpath = t7_user_list_fpath_by_dpath(dpath)
    user_count = 0
    with ExitStack() as stack:
        files = [stack.enter_context(gzip.open(fp, 'rt')) for fp in filepaths]
        outfile = stack.enter_context(gzip.open(output_fpath, 'wt'))
        current_lines = [f.readline() for f in files]
        min_user = None

        def _get_min_user_and_indices():
            min_user = min(current_lines)
            return min_user, [
                i for i in range(len(current_lines))
                if current_lines[i] == min_user
            ]

        def _increment_pointer(i):
            line = files[i].readline()
            if len(line) < 1:
                current_lines[i] = DONE_MARKER
                return
            current_lines[i] = line

        while any(current_lines):
            min_user, min_ixs = _get_min_user_and_indices()
            if min_user == DONE_MARKER:
                break
            # no need for a linebreak here; already here
            outfile.write('{}'.format(min_user))
            for i in min_ixs:
                _increment_pointer(i)
            user_count += 1

    gc.collect()

    qprint(f"{user_count} twitter7 users dumped into {output_fpath}")
    qprint("Sorting user file...")
    sorted_output_fpath = t7_user_list_fpath_by_dpath(dpath, sorted=True)
    sort_username_file(
        input_fpath=output_fpath, output_fpath=sorted_output_fpath)
    qprint("User file sorted!")
    qprint((f"{user_count:,} twitter7 users dumped into {output_fpath}"
            f" and {sorted_output_fpath}"))


def _uname_and_tweets_from_line(line):
    if len(line) < 1:
        return '', ''
    ix = line.find(' ')
    return line[0:ix], line[ix+1:]


DUMP_FNAME_RGX = '[\w\d_\-]+{}[\w\d_\.]+'.format(DUMP_FNAME_MARKER)


def merge_dump_files(dpath):
    qprint("\nStarting to merge all twitter7 tweet dumps in {}".format(dpath))
    filepaths = [
        os.path.join(dpath, fname) for fname in os.listdir(dpath)
        if re.match(pattern=DUMP_FNAME_RGX, string=fname)
    ]
    qprint("Found {} files to merge.".format(len(filepaths)))
    output_fpath = twitter7_tweet_list_fpath_by_dpath(dpath)
    user_count = 0
    with ExitStack() as stack:
        files = [stack.enter_context(gzip.open(fp, 'rt')) for fp in filepaths]
        outfile = stack.enter_context(gzip.open(output_fpath, 'wt'))
        current_lines = [f.readline() for f in files]
        current_users, current_tweets = list(zip(*[
            _uname_and_tweets_from_line(line)
            for line in current_lines
        ]))
        current_users = list(current_users)
        current_tweets = list(current_tweets)

        def _get_min_user_and_indices():
            min_user = min(current_users)
            return min_user, [
                i for i in range(len(current_users))
                if current_users[i] == min_user
            ]

        def _increment_pointer(i):
            line = files[i].readline()
            if len(line) < 1:
                current_lines[i] = DONE_MARKER
                current_users[i] = DONE_MARKER
                current_tweets[i] = DONE_MARKER
                return
            current_lines[i] = line
            current_users[i], current_tweets[i] = _uname_and_tweets_from_line(
                line)

        min_user = None
        while any(current_lines):
            min_user, min_ixs = _get_min_user_and_indices()
            if min_user == DONE_MARKER:
                break
            min_tweets_sets = [current_tweets[i] for i in min_ixs]
            min_user_tweets = ' '.join(min_tweets_sets)
            min_user_tweets = min_user_tweets.replace('\n', ' ')
            outfile.write('{} {}\n'.format(min_user, min_user_tweets))
            for i in min_ixs:
                _increment_pointer(i)
            user_count += 1
    qprint("Finished merging tweet files. {} users found.".format(user_count))

    # qprint("Sorting tweets file...")
    # sorted_output_fpath = twitter7_tweet_list_fpath_by_dpath(
    #     dpath, sorted=True)
    # sort_username_file(
    #     input_fpath=output_fpath, output_fpath=sorted_output_fpath)
    # qprint("Tweets file sorted!")
    # qprint((f"twitter7 tweets dumped into {output_fpath}"
    #         " and {sorted_output_fpath}"))


def phase1(output_dpath, tpath=None, subphases=None):
    """Splits a raw twitter7 tweets file into user-merged subset files.

    Parameters
    ----------
    output_dpath : str
        The path to the designated output folder.
    tpath : str, optional
        The path to the twitter7 dataset folder. If not given, the value keyed
        to 'twitter7_dpath' is looked up in the twikwak17 configuration file.
    subphases : list of str, optional
        If given, only subphases matching given strings are ran. E.g. '2.1'.
    """
    start = time.time()
    if tpath is None:
        tpath = twitter7_dpath()
    os.makedirs(output_dpath, exist_ok=True)
    output_report_fpath = phase_output_report_fpath(1, output_dpath)

    with open(output_report_fpath, 'wt+') as output_report_f:
        set_output_report_file_handle(output_report_f)
        qprint("\n\n====== PHASE 1 =====")
        qprint("Starting phase 1 from {} input dir to {} output dir.".format(
                tpath, output_dpath))

        if (subphases is None) or ('1.1' in subphases):
            qprint("\n\n---- 1.1 ----\nOrdering tweets by user per-file...")
            for fname in os.listdir(tpath):
                if not re.match(
                        pattern=DEF_TWITTER7_FNAME_PATTERN, string=fname):
                    print(f"Skipping file {fname}; no pattern match.")
                    continue
                order_tweets_by_user_in_file(
                    fpath=os.path.join(tpath, fname),
                    output_dpath=output_dpath,
                )

        if (subphases is None) or ('1.2' in subphases):
            qprint("\n\n---- 1.2 ----\nMerging user files...")
            merge_user_files(output_dpath)

        if (subphases is None) or ('1.3' in subphases):
            qprint("\n\n---- 1.3 ----\nMerging tweet files...")
            merge_dump_files(output_dpath)

        qprint("\n\n====== END-OF PHASE 1 ======")
        end = time.time()
        print((
            "Finished running phase 1 of the twikwak17 pipeline.\n"
            "Run duration: {}".format(seconds_to_duration_str(end - start))
        ))
    set_output_report_file_handle(None)
    create_timestamped_report_file_copy(output_report_fpath)
