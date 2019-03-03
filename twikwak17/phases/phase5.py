"""Phase 5 of the twikwak17 dataset generation process."""

import re
import gc
import time
import gzip
from contextlib import ExitStack


from twikwak17.shared import (
    qprint,
    uname2id_fpath_by_dpath,
    uname_to_gender_map_fpath_by_dpath,
    uid_to_gender_map_fpath_by_dpath,
    seconds_to_duration_str,
    phase_output_report_fpath,
    set_output_report_file_handle,
    create_timestamped_report_file_copy,
)


UNAME_TO_ID_REGEX = '(\s*\S+) (\S+)'


def get_uname2uid_map(uname2id_fpath):
    qprint("\nLoading uname2uid map from file...")
    lines_read = 0
    matching_lines = 0
    nonmatching_lines = 0
    uname2id_map = {}
    with gzip.open(uname2id_fpath, 'rt') as uname2id_f:
        for line in uname2id_f:
            lines_read += 1
            try:
                uname, uid = re.findall(UNAME_TO_ID_REGEX, line)[0]
                uname2id_map[uname] = uid
                matching_lines += 1
            except IndexError:
                nonmatching_lines += 1
            if lines_read % 100000 == 0:
                qprint((
                    f"{lines_read:,} lines read; {matching_lines:,}"
                    " lines matched."), end='\r')
    qprint("uname2uid map loaded from file successfully.\n")
    return uname2id_map


UNAME_TO_GENDER_REGEX = '(\s*\S+) ([01])'


def uname_and_gender_from_line(line):
    """Breaks down a uname-to-gender line into username and gender digit.

    Parameters
    ----------
    line : str
        A twitter7 merged-by-user line of the format:
        "_some_female_usernAme 1"

    Returns
    -------
    user, gender : str, str
        A 2-tuple of the user name string and a one-character string of the
        user's predicted gender. E.g. ('_some_female_usernAme", "1")
    """
    if len(line) < 1:
        return None, None
    user, gender = re.findall(UNAME_TO_GENDER_REGEX, line)[0]
    return user, gender


def convert_uname2gender_map_to_uid2gender_map(
        uname_to_gender_map_fpath, uname2id_fpath, output_fpath):
    """Converts a username-to-gender mapping file to a user-id-to-gender one.

    Parameters
    ----------
    uname_to_gender_map_fpath : str
        The full qualified path to the username-to-gender file.
    uname2id_fpath : str
        The full qualified path to the username to user id mapping file.
    output_fpath : str
        The path to the designated output file.
    """
    qprint((
        "\nStarting to convert username-to-gender mapping in "
        f"{uname_to_gender_map_fpath} to a user-id-to-gender mapping using "
        f"uname-to-id map {uname2id_fpath}. Writing result to {output_fpath}."
    ))
    uname2id = get_uname2uid_map(uname2id_fpath)
    with ExitStack() as stack:
        uname2g_f = stack.enter_context(
            gzip.open(uname_to_gender_map_fpath, 'rt'))
        out_f = stack.enter_context(gzip.open(output_fpath, 'wt+'))
        lines_read = 0
        lines_dumped = 0
        users_not_found = 0
        lines_to_dump = []

        uname2gender_line = uname2g_f.readline()
        lines_read += 1

        while uname2gender_line:
            uname, gender = uname_and_gender_from_line(uname2gender_line)
            try:
                uid = uname2id[uname]
                lines_to_dump.append(f"{uid} {gender}")
                if len(lines_to_dump) >= 100000:
                    lines = "\n".join(lines_to_dump) + "\n"
                    out_f.write(lines)
                    lines_dumped += 100000
                    lines_to_dump = None
                    del lines_to_dump
                    gc.collect()
                    lines_to_dump = []
            except KeyError:
                users_not_found += 1
            uname2gender_line = uname2g_f.readline()
            lines_read += 1
            if lines_read % 10000 == 0:
                qprint((
                    f"{lines_read:,} lines read|"
                    f"{lines_dumped:,} lines dumped|"
                    f"{users_not_found:,} users not found. {uname} ~ {uid}"))
        if len(lines_to_dump) > 0:
            lines = "\n".join(lines_to_dump) + "\n"
            out_f.write(lines)
            lines_dumped += len(lines_to_dump)
        return int(lines_dumped)


def phase5(phase2_output_dpath, phase4_output_dpath, phase5_output_dpath):
    """Build a sorted username list of the intersection of twitter7 and kwak10.

    Parameters
    ----------
    phase2_output_dpath : str
        The path to the output directory of phase 2.
    phase4_output_dpath : str
        The path to the output directory of phase 4.
    phase5_output_dpath : str
        The path to the output directory of this phase, phase 5.
    """
    start = time.time()
    uname2id_fpath = uname2id_fpath_by_dpath(
        phase2_output_dpath)
    uname_to_gender_map_fpath = uname_to_gender_map_fpath_by_dpath(
        phase4_output_dpath)
    output_fpath = uid_to_gender_map_fpath_by_dpath(phase5_output_dpath)
    output_report_fpath = phase_output_report_fpath(5, phase5_output_dpath)

    with open(output_report_fpath, 'wt+') as output_report_f:
        set_output_report_file_handle(output_report_f)
        qprint("\n\n====== PHASE 5 =====")
        qprint((
            f"Starting phase 5 from \n{uname2id_fpath} and "
            f"\n{uname_to_gender_map_fpath} \ninput files to {output_fpath} "
            "output file."))

        user_count = convert_uname2gender_map_to_uid2gender_map(
            uname_to_gender_map_fpath=uname_to_gender_map_fpath,
            uname2id_fpath=uname2id_fpath,
            output_fpath=output_fpath,
        )

        qprint((
            f"Translated username to uid of {user_count:,} users in"
            f" username-to-gender map; dumped to {output_fpath}."))

        end = time.time()
        qprint((
            "Finished running phase 5 of the twikwak17 pipeline.\n"
            "Run duration: {}".format(seconds_to_duration_str(end - start))
        ))
    set_output_report_file_handle(None)
    create_timestamped_report_file_copy(output_report_fpath)
