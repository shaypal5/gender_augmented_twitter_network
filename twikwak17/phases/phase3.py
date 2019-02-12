"""Phase 3 of the twikwak17 dataset generation pipeline."""

import os
import re
import time
import gzip
import subprocess
import multiprocessing
from contextlib import ExitStack
from psutil import virtual_memory

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

# see full documentation for GNU sort in
# http://www.gnu.org/software/coreutils/manual/html_node/sort-invocation.html
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
    output_report_fpath = phase_output_report_fpath(3, phase3_output_dpath)

    with ExitStack() as stack:
        output_report_f = stack.enter_context(open(output_report_fpath, 'wt+'))
        set_output_report_file_handle(output_report_f)

        qprint("\n\n====== PHASE 3 =====")
        qprint((
            f"Starting phase 3 from \n{phase1_output_dpath} "
            f"\n{phase2_output_dpath} \ninput directoris to the "
            f"{phase3_output_dpath} output dir."))

        # sort username files
        qprint("Sorting username files..")
        sorted_t7_unames_fpath = sort_username_file(t7_unames_fpath)
        sorted_k10_unames_fpath = sort_username_file(k10_unames_fpath)
        qprint("Done sorting username files!")

        t7_f = stack.enter_context(gzip.open(sorted_t7_unames_fpath, 'rt'))
        k10_f = stack.enter_context(gzip.open(sorted_k10_unames_fpath, 'rt'))
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
