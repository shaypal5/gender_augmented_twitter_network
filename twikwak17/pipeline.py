"""Running the entire data generation pipeline."""

from .phases import (
    phase1,
)
from .shared import (
    TWIK_CFG,
    TWIK_CFG_FPATH,
    CfgKey,
    phase_output_dpath,
    qprint,
)
from .exceptions import (
    TwikwakConfigurationError,
)


MISSING_VAL_TEMP = (
    "Missing configuration value for {}. Either provide it through the CLI "
    "call or configure it at {}".format(TWIK_CFG_FPATH))


def run_pipeline(tpath=None, kpath=None, output_dpath=None):
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
    """
    if tpath is None:
        try:
            tpath = TWIK_CFG[CfgKey.TWITTER7_DPATH]
        except KeyError:
            raise TwikwakConfigurationError(MISSING_VAL_TEMP.format(
                'twitter7_dpath'))
    qprint((
        "Starting to run the entire twikwak17 dataset generation pipeline."
        "\nPath to twitter7 dataset folder: {}\n"
        "Path to kwak10www dataset folder: {}\n"
        "Path to output folder: {}"
    ).format(tpath, kpath, output_dpath))
    phase1_out_dpath = phase_output_dpath(1, output_dpath)
    # phase1(tpath, output_dpath)
