"""Generate a sample of twitter7 to test the pipeline."""

from twikwak17.shared import TWIK_CFG


def sample_twitter7(
        num_tweets, source_fpath=None, target_fpath=None, silent=False):
    """Generate a sample of twitter7 to test the pipeline."""
    print("Generating a sample of {} tweets from {}, writing to {}".format(
        num_tweets, source_fpath, target_fpath))
