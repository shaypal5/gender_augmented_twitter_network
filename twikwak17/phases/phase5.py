"""Phase 5 of the twikwak17 dataset generation process."""

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
    phase_output_report_fpath,
    set_output_report_file_handle,
    create_timestamped_report_file_copy,
)


UNAME_REGEX = '\s*\S+'


def uname_and_tweets_from_line(line):
    """Breaks down a twitter7 merged-by-user line into user and tweets.

    Parameters
    ----------
    line : str
        A twitter7 merged-by-user line of the format:
        "_some_usernAme contents of tweets i love fish"

    Returns
    -------
    user, tweets : str, str
        A 2-tuple of the user name string and a string of all tweets.
    """
    if len(line) < 1:
        return None, None
    user = re.findall(UNAME_REGEX, line)[0]
    return user, line[len(user)+1:]


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
        "\nStarting to classify gender of users in {} by tweets in {}; "
        "Dumping into {}."
    ).format(
        user_intersection_fpath, twitter7_tweets_by_user_fpath,
        output_fpath,
    ))
    with ExitStack() as stack:
        tweets_f = stack.enter_context(
            gzip.open(twitter7_tweets_by_user_fpath, 'rt'))
        intrsct_f = stack.enter_context(
            gzip.open(user_intersection_fpath, 'rt'))
        out_f = stack.enter_context(gzip.open(output_fpath, 'wt'))
        t7_lines_read = 0
        intersection_lines_read = 0
        users_read = 0
        users_matched = 0
        users_dumped = 0
        users_and_genders_to_dump = []

        t7_line = tweets_f.readline()
        t7_lines_read += 1
        intrsct_line = intrsct_f.readline()
        intersection_lines_read += 1

        while t7_line and intrsct_line:
            t7_user, tweets = uname_and_tweets_from_line(t7_line)
            list_user = re.findall(UNAME_REGEX, intrsct_line)[0]
            list_user = list_user.lower()
            if t7_user == list_user:
                gender = predict_gender_by_tweets(tweets)
                users_and_genders_to_dump.append(f"{t7_user} {gender}")
                users_matched += 1
                if users_matched % 100000 == 0:
                    out_f.writelines(users_and_genders_to_dump)
                    users_dumped += 100000
                    users_and_genders_to_dump = None
                    del users_and_genders_to_dump
                    gc.collect()
                    users_and_genders_to_dump = []
                t7_line = tweets_f.readline()
                t7_lines_read += 1
                intrsct_line = intrsct_f.readline()
                intersection_lines_read += 1
            elif t7_user < list_user:
                t7_line = tweets_f.readline()
                t7_lines_read += 1
            else:
                intrsct_line = intrsct_f.readline()
                intersection_lines_read += 1
            users_read += 1
            if users_read % 5000 == 0:
                qprint((
                    f"{t7_lines_read:,} t7 lines read|"
                    f"{intersection_lines_read:,} ∩ lines read|"
                    f"{users_read:,} users read; {users_matched:,} matched|"
                    f"{t7_user} ~ {list_user}"))
            # if users_read % 100000 == 0:
            #     print(f"|{t7_user}|{list_user}")
        if len(users_and_genders_to_dump) > 0:
            out_f.writelines(users_and_genders_to_dump)
            users_dumped += len(users_and_genders_to_dump)
        return int(users_dumped)


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
        phase1_output_dpath, sorted=False)
    user_intersection_fpath = uname_intersection_fpath_by_dpath(
        phase3_output_dpath)
    output_fpath = uname_to_gender_map_fpath_by_dpath(phase4_output_dpath)
    output_report_fpath = phase_output_report_fpath(4, phase4_output_dpath)

    with open(output_report_fpath, 'wt+') as output_report_f:
        set_output_report_file_handle(output_report_f)
        qprint("\n\n====== PHASE 4 =====")
        qprint((
            f"Starting phase 4 from \n{t7_tweets_by_user_fpath} and "
            f"\n{user_intersection_fpath} \ninput files to {output_fpath} "
            "output file."))

        user_count = gender_classify_users_in_intersection_by_twitter7(
            t7_tweets_by_user_fpath,
            user_intersection_fpath,
            output_fpath,
        )

        qprint((
            f"{user_count:,} users gender classified;"
            f" dumped to {output_fpath}."))

        end = time.time()
        qprint((
            "Finished running phase 4 of the twikwak17 pipeline.\n"
            "Run duration: {}".format(seconds_to_duration_str(end - start))
        ))
    set_output_report_file_handle(None)
    create_timestamped_report_file_copy(output_report_fpath)
