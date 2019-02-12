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
    phase_output_report_fpath,
    DONE_MARKER,
    set_output_report_file_handle,
    create_timestamped_report_file_copy,
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
    t7_unames_fpath = t7_user_list_fpath_by_dpath(
        phase1_output_dpath, sorted=True)
    k10_unames_fpath = kwak10_unames_fpath_by_dpath(
        phase2_output_dpath, sorted=True)
    uname_out_fpath = uname_intersection_fpath_by_dpath(phase3_output_dpath)
    output_report_fpath = phase_output_report_fpath(3, phase3_output_dpath)

    with ExitStack() as stack:
        output_report_f = stack.enter_context(open(output_report_fpath, 'wt+'))
        set_output_report_file_handle(output_report_f)

        qprint("\n\n====== PHASE 3 =====")
        qprint((
            f"Starting phase 3 from \n{phase1_output_dpath} "
            f"\n{phase2_output_dpath} \ninput directoris to the "
            f"{phase3_output_dpath} output dir."))

        t7_f = stack.enter_context(gzip.open(t7_unames_fpath, 'rt'))
        k10_f = stack.enter_context(gzip.open(k10_unames_fpath, 'rt'))
        files = [t7_f, k10_f]
        out_f = stack.enter_context(gzip.open(uname_out_fpath, 'wt+'))
        current_lines = [None, None]
        user_count = 0
        line_count = 0
        uname_regex = '\s*\S+'

        def _increment_pointer(i):
            nonlocal line_count
            line = files[i].readline()
            line_count += 1
            if len(line) < 1:
                current_lines[i] = DONE_MARKER
            elif line == "\n" or line == "\r":
                _increment_pointer(i)
            else:
                current_lines[i] = line

        _increment_pointer(0)
        _increment_pointer(1)
        while any(current_lines):
            # print("|{}|{}|".format(current_lines[0], current_lines[1]))
            if any([line == DONE_MARKER for line in current_lines]):
                break
            users = [
                re.findall(uname_regex, line)[0]
                for line in current_lines
            ]
            # print("||{}||{}||".format(users[0], users[1]))
            if users[0] == "_":
                _increment_pointer(0)
            elif users[0] == users[1]:
                out_f.write('{}\n'.format(users[0]))
                _increment_pointer(0)
                _increment_pointer(1)
                user_count += 1
            else:
                if users[0] < users[1]:
                    _increment_pointer(0)
                else:
                    _increment_pointer(1)
            if line_count % 10000 == 0:
                print((f" {line_count:,} lines read, {user_count:,} "
                       f"users dumped.[{users[0]}][{users[1]}]"), end="\r")

        qprint((f"{user_count:,} intersection users dumped into "
                f"{uname_out_fpath}."))

        end = time.time()
        print((
            "Finished running phase 3 of the twikwak17 pipeline.\n"
            "Run duration: {}".format(seconds_to_duration_str(end - start))
        ))
        set_output_report_file_handle(None)
        create_timestamped_report_file_copy(output_report_fpath)
