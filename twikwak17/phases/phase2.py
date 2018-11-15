"""Phase 1 of the twikwak17 dataset generation process."""

import os
import re
import time
import gzip
from psutil import virtual_memory
from contextlib import ExitStack

from sortedcontainers import SortedDict

from twikwak17.shared import (
    qprint,
    kwak10_dpath,
    uname2id_fpath_by_kpath,
    kwak10_unames_fpath_by_kpath,
    seconds_to_duration_str,
)


ULIST_FNAME = 'numeric2screen'
LINE_REGEX = '([0-9]+) (.+)'
BYTES_IN_MB = 1000000
MIN_AVAIL_MEM_MB_DEF = 500
USR_FNAME_MARKER = 'p2usr'
LINE_DUMP_FREQ = 5000000
UNAME2ID_REGEX = '(.+) ([0-9]+)'


def _dump_uname2id(uname_2_id, files_written, output_dpath):
        dump_fpath = '{}/{}_{}.txt.gz'.format(
            output_dpath, USR_FNAME_MARKER, files_written)
        with gzip.open(dump_fpath, 'wt+') as f:
            for uname, uid in uname_2_id.items():
                f.write('{} {}\n'.format(uname, uid))


def inverse_numeric2screen_into_multiple_files(output_dpath, kpath):
    min_mem_bytes = MIN_AVAIL_MEM_MB_DEF * BYTES_IN_MB
    files_written = 0
    ulist_fpath = os.path.join(kpath, ULIST_FNAME)
    uname_to_id = SortedDict()
    i = 0
    with open(ulist_fpath, 'rt') as f:
        for line in f:
            match_groups = re.match(LINE_REGEX, line).groups()
            uid = match_groups[0]
            uname = match_groups[1]
            uname_to_id[uname] = uid
            i += 1
            if i % 10000 == 0:
                print("{:,} lines read".format(i), end="\r")
                av_mem = virtual_memory().available
                if av_mem < min_mem_bytes or (i % LINE_DUMP_FREQ == 0):
                    _dump_uname2id(uname_to_id, files_written, output_dpath)
                    files_written += 1
                    uname_to_id = SortedDict()
                    qprint("\bFile dumped.\n")
    qprint("{} files written.".format(files_written))


USR_FNAME_RGX = '{}_[\d]+.txt.gz'.format(USR_FNAME_MARKER)


def merge_user_files(input_dpath, uname_fpath, uname2id_fpath):
    qprint("Starting to merge all kwak10 user lists in {}".format(
        input_dpath))
    filepaths = [
        os.path.join(input_dpath, fname) for fname in os.listdir(input_dpath)
        if re.match(pattern=USR_FNAME_RGX, string=fname)
    ]
    qprint("Found {} files to merge.".format(len(filepaths)))
    user_count = 0
    with ExitStack() as stack:
        files = [stack.enter_context(gzip.open(fp, 'rt')) for fp in filepaths]
        uname_f = stack.enter_context(gzip.open(uname_fpath, 'wt'))
        uname2id_f = stack.enter_context(gzip.open(uname2id_fpath, 'wt'))
        current_lines = [f.readline() for f in files]
        min_user = None

        def _get_min_user_id():
            min_line = min(current_lines).replace('\n', '')
            match_groups = re.match(UNAME2ID_REGEX, min_line)
            ix = [
                i for i in range(len(current_lines))
                if current_lines[i] == min_user
            ]
            return match_groups[0], match_groups[1]

        def _increment_pointer(i):
            line = files[i].readline()
            if len(line) < 1:
                current_lines[i] = DONE_MARKER
                return
            current_lines[i] = line

        while any(current_lines):
            min_user, min_id = _get_min_user_and_id()
            if min_user == DONE_MARKER:
                break
            # no need for a linebreak here; already here
            uname_f.write('{}\n'.format(min_user))
            uname2id_f.write('{} {}\n'.format(min_user, min_id))
            for i in min_ixs:
                _increment_pointer(i)
            user_count += 1
    qprint("{} twitter7 users dumped into {}".format(
        user_count, output_fpath))


def phase2(output_dpath, kpath=None):
    """Lexicographically sorts the numerically sorted numeric2screen user list.

    Parameters
    ----------
    output_dpath : str
        The path to the designated output folder.
    kpath : str, optional
        The path to the kwak10www dataset folder. If not given, the value keyed
        to 'kwak10_dpath' is looked up in the twikwak17 configuration file.
    """
    start = time.time()
    if kpath is None:
        kpath = kwak10_dpath()
    os.makedirs(output_dpath, exist_ok=True)
    qprint("\n\n====== PHASE 2 =====")
    qprint("Starting phase 2 from {} input dir to {} output dir.".format(
            kpath, output_dpath))

    qprint("\n\n---- 2.1 ----\nInverting numeric2screen into several files...")
    inverse_numeric2screen_into_multiple_files(output_dpath, kpath)

    unames_fpath = kwak10_unames_fpath_by_kpath(kpath)
    qprint("\n\n---- 2.2 ----\nDumping user name list to {}...".format(
        unames_fpath))

    uname2id_fpath = uname2id_fpath_by_kpath(kpath)
    qprint("\n\n---- 2.2 ----\nDumping user-to-id map to {}...".format(
        uname2id_fpath))

    print("\n\n====== END-OF PHASE 2 ======")
    end = time.time()
    print((
        "Finished running phase 2 of the twikwak17 pipeline.\n"
        "Run duration: {}".format(seconds_to_duration_str(end - start))
    ))
