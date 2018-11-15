"""Phase 1 of the twikwak17 dataset generation process."""

import os

from twikwak17.shared import (
    qprint,
    kwak10_dpath,
)


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
    if kpath is None:
        kpath = kwak10_dpath()
    os.makedirs(output_dpath, exist_ok=True)
    qprint("\n\n====== PHASE 2 =====")
    qprint("Starting phase 2 from {} input dir to {} output dir.".format(
            kpath, output_dpath))
    qprint("\n\n====== END-OF PHASE 2 ======")
