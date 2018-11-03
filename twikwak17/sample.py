"""Generate a sample of twitter7 to test the pipeline."""

import os
import gzip

from twikwak17.shared import (
    TWIK_CFG,
    DEF_FNAMES,
    default_source_dpath,
    sample_dpath_by_source_dpath,
)


def sample_twitter7_file(
        num_tweets, source_fpath=None, target_fpath=None, quiet=False):
    """Generates a sample of a twitter7 file (usefull for pipeline testing).

    Parameters
    ----------
    num_tweets : int
        The sample size, in tweets.
    source_fpath : str, optional
        The full path to the source file to sample. If not given, the source
        directory path is looked up in twikwak 'source_dir' configuration value
        and the first archive file is used (assuming the default file name).
    target_fpath : str, optional
        The full path to the target file into which to write the sample.
        If not given, a file named `twitter7_sample.txt.gz` is created in the
        directory of the source file used.
    quiet : boolean, default False
        Is set to True, all messages are silenced.
    """
    _print = (lambda x: x) if quiet else print
    if source_fpath is None:
        source_dpath = TWIK_CFG['source_dir']
        source_fpath = os.path.join(source_dpath, DEF_FNAMES[0])
    else:
        source_dpath = os.path.dirname(source_fpath)
    if target_fpath is None:
        target_fpath = os.path.join(source_dpath, 'twitter7_sample.txt.gz')
    _print("Generating a sample of {} tweets from {}, writing to {}".format(
        num_tweets, source_fpath, target_fpath))
    with gzip.open(source_fpath, 'rt') as sourcef:
        with gzip.open(target_fpath, 'wt') as targetf:
            for i, line in enumerate(sourcef):
                targetf.write(line)
                if i >= num_tweets * 4:
                    print("Sample file generated. Terminating.")
                    return


def sample_twitter7_folder(
        num_tweets, source_dpath=None, quiet=False):
    """Generates a sample of a twitter7 folder (usefull for pipeline testing).

    Parameters
    ----------
    num_tweets : int
        The sample size, in tweets, per file.
    source_dpath : str, optional
        The full path to the source folder to sample. If not given, the source
        folder path is looked up in twikwak 'source_dir' configuration value.
    quiet : boolean, default False
        Is set to True, all messages are silenced.
    """
    _print = (lambda x: x) if quiet else print
    if source_dpath is None:
        source_dpath = default_source_dpath()
    target_dpath = sample_dpath_by_source_dpath(source_dpath)
    _print((
        "Generating a sample of {} tweets per file from {},"
        " writing sample files to {}").format(
            num_tweets, source_dpath, target_dpath))
