"""Phase 1 of the twikwak17 dataset generation process."""

import re
import gc
import time
import gzip
from contextlib import ExitStack

from speks import predict_gender_by_tweets

from twikwak17.shared import (
    qprint,
    twitter7_tweet_list_fpath_by_dpath,
    uname_intersection_fpath_by_dpath,
    uname_to_gender_map_fpath_by_dpath,
    seconds_to_duration_str,
)


def _uname_and_tweets_from_line(line):
    if len(line) < 1:
        return None, None
    ix = line.find(' ')
    return line[0:ix], line[ix+1:]


def gender_classify_users_in_intersection_by_twitter7(
        twitter7_tweets_by_user_fpath, user_intersection_fpath, output_fpath):
    """Gender classifies twitter users by the content of their tweets.

    Only users in the intersection of the twitter7 and kwak10www datasets are
    classified. The gender of the users is estimated by the content of their
    tweets in the twitter7 dataset.

    Parameters
    ----------
    twitter7_tweets_by_user_fpath : str
        The full qualified path to the twitter7 tweets-by-user file.
    user_intersection_fpath : str
        The full qualified path to the user list file.
    output_dpath : str
        The path to the designated output folder.
    """
    qprint((
        "\nStarting to classify gender of users in {} by tweets in {};"
        "Dumping into {}."
    ).format(
        user_intersection_fpath, twitter7_tweets_by_user_fpath,
        output_fpath,
    ))
    with ExitStack() as stack:
        tweets_f = stack.enter_context(
            gzip.open(twitter7_tweets_by_user_fpath, 'rt'))
        users_f = stack.enter_context(gzip.open(user_intersection_fpath, 'rt'))
        out_f = stack.enter_context(gzip.open(output_fpath, 'wt'))
        uname_regex = '[^\s]+'
        user_count = 0
        users_and_genders_to_dump = []

        t7_line = tweets_f.readline()
        list_line = users_f.readline()

        while t7_line and list_line:
            t7_user, tweets = _uname_and_tweets_from_line(tweets_f.readline())
            list_user = re.findall(uname_regex, list_line)[0]
            if t7_user == list_user:
                gender = predict_gender_by_tweets(tweets)
                users_and_genders_to_dump.append(f"{t7_user} {gender}")
                user_count += 1
                if user_count % 100000 == 0:
                    out_f.writelines(users_and_genders_to_dump)
                    users_and_genders_to_dump = None
                    del users_and_genders_to_dump
                    gc.collect()
                    users_and_genders_to_dump = []
                    print("{} users dumped".format(user_count), end="\r")
                t7_line = tweets_f.readline()
                list_line = users_f.readline()
            elif t7_user < list_user:
                t7_line = tweets_f.readline()
            else:
                list_line = users_f.readline()
        return user_count


def phase4(phase1_output_dpath, phase3_output_dpath, phase4_output_dpath):
    """Build a sorted username list of the intersection of twitter7 and kwak10.

    Parameters
    ----------
    phase1_output_dpath : str
        The path to the output directory of phase 1.
    phase3_output_dpath : str
        The path to the output directory of phase 3.
    phase4_output_dpath : str
        The path to the output directory of this phase, phase 4.
    """
    start = time.time()
    t7_tweets_by_user_fpath = twitter7_tweet_list_fpath_by_dpath(
        phase1_output_dpath)
    user_intersection_fpath = uname_intersection_fpath_by_dpath(
        phase3_output_dpath)
    output_fpath = uname_to_gender_map_fpath_by_dpath(phase4_output_dpath)

    qprint("\n\n====== PHASE 4 =====")
    qprint((
        "Starting phase 4 from \n{t7_tweets_by_user_fpath} and "
        "\n{user_intersection_fpath} \ninput files to {output_fpath}"
        "output file."))

    user_count = gender_classify_users_in_intersection_by_twitter7(
        t7_tweets_by_user_fpath,
        user_intersection_fpath,
        output_fpath,
    )

    qprint(f"{user_count} users gender classified; dumped to {output_fpath}.")

    end = time.time()
    print((
        "Finished running phase 3 of the twikwak17 pipeline.\n"
        "Run duration: {}".format(seconds_to_duration_str(end - start))
    ))
