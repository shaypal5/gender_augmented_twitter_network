"""Phase 3 of the twikwak17 dataset generation pipeline."""

import re
import time
import gzip
from contextlib import ExitStack

from twikwak17.shared import (
    qprint,
    seconds_to_duration_str,
    t7_user_list_fpath_by_dpath,
    kwak10_unames_fpath_by_dpath,
    uname_intersection_fpath_by_dpath,
    DONE_MARKER,
)


def phase3(phase1_output_dpath, phase2_output_dpath, phase3_output_dpath):
    """Build a sorted username list of the intersection of twitter7 and kwak10.

    Parameters
    ----------
    phase1_output_dpath : str
        The path to the output directory of phase 1.
    phase2_output_dpath : str
        The path to the output directory of phase 2.
    phase3_output_dpath : str
        The path to the output directory of this phase, phase 3.
    """
    start = time.time()
    t7_unames_fpath = t7_user_list_fpath_by_dpath(phase1_output_dpath)
    k10_unames_fpath = kwak10_unames_fpath_by_dpath(phase2_output_dpath)
    uname_out_fpath = uname_intersection_fpath_by_dpath(phase3_output_dpath)

    qprint("\n\n====== PHASE 3 =====")
    qprint(
        "Starting phase 3 from \n{} \n{} \ninput dir to {} output dir.".format(
            phase1_output_dpath, phase2_output_dpath, phase3_output_dpath))

    with ExitStack() as stack:
        t7_f = stack.enter_context(gzip.open(t7_unames_fpath, 'rt'))
        k10_f = stack.enter_context(gzip.open(k10_unames_fpath, 'rt'))
        files = [t7_f, k10_f]
        out_f = stack.enter_context(gzip.open(uname_out_fpath, 'wt+'))
        current_lines = [f.readline() for f in files]
        user_count = 0
        uname_regex = '[^\s]+'

        def _increment_pointer(i):
            line = files[i].readline()
            if len(line) < 1:
                current_lines[i] = DONE_MARKER
                return
            current_lines[i] = line

        while any(current_lines):
            # print("{}|{}".format(current_lines[0], current_lines[1]))
            if any([line == DONE_MARKER for line in current_lines]):
                break
            for i in [0, 1]:
                x = current_lines[i]
                if x == "\n" or x == "\r":
                    _increment_pointer(i)
                    continue
            users = [
                re.findall(uname_regex, line)[0]
                for line in current_lines
            ]
            # print("{}||{}".format(users[0], users[1]))
            if users[0] == users[1]:
                out_f.write('{}\n'.format(users[0]))
                _increment_pointer(0)
                _increment_pointer(1)
                user_count += 1
                if user_count % 10000 == 0:
                    print("{} users dumped".format(user_count), end="\r")
            else:
                if users[0] < users[1]:
                    _increment_pointer(0)
                else:
                    _increment_pointer(1)

    qprint("{} intersection users dumped into {}.".format(
        user_count, uname_out_fpath))

    end = time.time()
    print((
        "Finished running phase 3 of the twikwak17 pipeline.\n"
        "Run duration: {}".format(seconds_to_duration_str(end - start))
    ))
