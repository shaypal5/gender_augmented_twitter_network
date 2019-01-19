"""Running the entire data generation pipeline."""

import re
import time

from .phases import (
    phase1,
    phase2,
    phase3,
    phase4,
)
from .shared import (
    CfgKey,
    error_raising_cfg_val_get,
    phase_output_dpath,
    qprint,
    seconds_to_duration_str,
    Session,
)


def run_pipeline(
        tpath=None, kpath=None, output_dpath=None, session_fpath=None):
    """Runs the entire data generation pipeline.

    Parameters
    ----------
    tpath : str, optional
        The path to the twitter7 dataset folder. If not given, the value keyed
        to 'twitter7_dpath' is looked up in the twikwak17 configuration file.
    kpath : str, optional
        The path to the kwak10www dataset folder. If not given, the value keyed
        to 'kwak10_dpath' is looked up in the twikwak17 configuration file.
    output_dpath : str, optional
        The path to the designated output folder. If not given, the value keyed
        to 'output_dpath' is looked up in the twikwak17 configuration file.
    session_fpath : str, optional
        The path to the save file of a previous session to continue. If not
        given, a new session is created.
    """
    phases = ['1', '2', '3']
    run_phases(
        phases=phases, tpath=tpath, kpath=kpath, output_dpath=output_dpath,
        session_fpath=session_fpath)


def run_phases(
        phases, tpath=None, kpath=None, output_dpath=None,
        session_fpath=None):
    """Runs the entire data generation pipeline.

    Parameters
    ----------
    phases : list of str
        The list of phases and subphases to run. E.g. ['2', '3.1', '4.2']
    tpath : str, optional
        The path to the twitter7 dataset folder. If not given, the value keyed
        to 'twitter7_dpath' is looked up in the twikwak17 configuration file.
    kpath : str, optional
        The path to the kwak10www dataset folder. If not given, the value keyed
        to 'kwak10_dpath' is looked up in the twikwak17 configuration file.
    output_dpath : str, optional
        The path to the designated output folder. If not given, the value keyed
        to 'output_dpath' is looked up in the twikwak17 configuration file.
    session_fpath : str, optional
        The path to the save file of a previous session to continue. If not
        given, a new session is created.
    """
    if session_fpath is None:
        print("\n\nStarting a new twikwak17 session.")
        start = time.time()
        kwargs = {'tpath': tpath, 'kpath': kpath, 'output_dpath': output_dpath}
        session = Session(
            start_time=start,
            kwargs=kwargs,
            phases=None,
            last_completed_subphase=None,
        )
        last_completed_phase = '0'
        # last_completed_subphase = None
    else:
        qprint(
            "\n\nRestoring twikwak17 session from {}...".format(session_fpath))
        session = Session.load(session_fpath)
        start = session.start_time
        phases = session.kwargs['phases']
        tpath = session.kwargs['tpath']
        kpath = session.kwargs['kpath']
        output_dpath = session.kwargs['output_dpath']

    tpath = error_raising_cfg_val_get(tpath, CfgKey.TWITTER7_DPATH)
    kpath = error_raising_cfg_val_get(kpath, CfgKey.KWAK10_DPATH)
    output_dpath = error_raising_cfg_val_get(
        output_dpath, CfgKey.OUTPUT_DPATH)
    qprint((
        "\n\n######## twikwak17 ######## \n\n"
        "Starting to run the twikwak17 dataset generation pipeline."
        "\nPhases to run: {}\n"
        "\nPath to twitter7 dataset folder: {}\n"
        "Path to kwak10www dataset folder: {}\n"
        "Path to output folder: {}"
    ).format(phases, tpath, kpath, output_dpath))

    # big_phases = set([x[0] for x in phases])

    phase1_out_dpath = phase_output_dpath(1, output_dpath)
    if '1' in phases and last_completed_phase < '1':
        phase1(output_dpath=phase1_out_dpath, tpath=tpath)
    else:
        one_subphases = [p for p in phases if re.match("1\.\d", p)]
        if len(one_subphases) > 0:
            phase1(output_dpath=phase1_out_dpath, tpath=tpath,
                   subphases=one_subphases)

    phase2_out_dpath = phase_output_dpath(2, output_dpath)
    if '2' in phases:
        phase2(output_dpath=phase2_out_dpath, kpath=kpath)
    else:
        two_subphases = [p for p in phases if re.match("2\.\d", p)]
        if len(two_subphases) > 0:
            phase2(output_dpath=phase2_out_dpath, kpath=kpath,
                   subphases=two_subphases)

    phase3_out_dpath = phase_output_dpath(3, output_dpath)
    if '3' in phases:
        phase3(
            phase1_output_dpath=phase1_out_dpath,
            phase2_output_dpath=phase2_out_dpath,
            phase3_output_dpath=phase3_out_dpath,
        )

    phase4_out_dpath = phase_output_dpath(4, output_dpath)
    if '4' in phases:
        phase4(
            phase1_output_dpath=phase1_out_dpath,
            phase3_output_dpath=phase3_out_dpath,
            phase4_output_dpath=phase4_out_dpath,
        )

    end = time.time()
    print((
        "\n\nFinished running the twikwak17 pipeline.\n"
        "Run duration: {}".format(seconds_to_duration_str(end - start))
    ))
