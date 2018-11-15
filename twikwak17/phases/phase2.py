"""Phase 1 of the twikwak17 dataset generation process."""

import os
import re
import time
import json

from twikwak17.shared import (
    qprint,
    kwak10_dpath,
    uname2id_fpath_by_kpath,
    kwak10_unames_fpath_by_kpath,
    seconds_to_duration_str,
)


ULIST_FNAME = 'numeric2screen'
LINE_REGEX = '([0-9]+) (.+)'


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

    qprint("\n\n---- 2.1 ----\nReading list into memory...")
    ulist_fpath = os.path.join(kpath, ULIST_FNAME)
    uname_to_id = {}
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

    unames_fpath = kwak10_unames_fpath_by_kpath(kpath)
    qprint("\n\n---- 2.2 ----\nDumping user name list to {}...".format(
        unames_fpath))
    with open(unames_fpath, 'w+') as f:
        json.dump(sorted(uname_to_id.keys()), f)

    uname2id_fpath = uname2id_fpath_by_kpath(kpath)
    qprint("\n\n---- 2.2 ----\nDumping user-to-id map to {}...".format(
        uname2id_fpath))
    with open(uname2id_fpath, 'w+') as f:
        json.dump(sorted(uname_to_id, f))

    print("\n\n====== END-OF PHASE 2 ======")
    end = time.time()
    print((
        "Finished running phase 2 of the twikwak17 pipeline.\n"
        "Run duration: {}".format(seconds_to_duration_str(end - start))
    ))
