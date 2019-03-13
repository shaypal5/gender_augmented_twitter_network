"""Phase 7 of the twikwak17 dataset generation process."""

# import re
# import gc
import time
# import gzip
# import zipfile
# from contextlib import ExitStack


from twikwak17.shared import (
    qprint,
    seconds_to_duration_str,
    phase_output_report_fpath,
    set_output_report_file_handle,
    create_timestamped_report_file_copy,
    kwak10_twitter_rv_fpath,
    uid_to_gender_map_fpath_by_dpath,
    social_graph_fpath_by_dpath,
)


def phase7(phase5_output_dpath, phase6_output_dpath, phase7_output_dpath):
    """Removes non-intersection edges from kwak10's social graph.

    Parameters
    ----------
    phase5_output_dpath : str
        The path to the output directory of phase 5.
    phase6_output_dpath : str
        The path to the output directory of phase 6.
    phase7_output_dpath : str
        The path to the output directory of this phase, phase 7.
    """
    start = time.time()
    twitter_rv_fpath = kwak10_twitter_rv_fpath()
    uid2gender_fpath = uid_to_gender_map_fpath_by_dpath(phase5_output_dpath)
    output_fpath = social_graph_fpath_by_dpath(phase7_output_dpath)
    output_report_fpath = phase_output_report_fpath(7, phase7_output_dpath)

    with open(output_report_fpath, 'wt+') as output_report_f:
        set_output_report_file_handle(output_report_f)
        qprint("\n\n====== PHASE 7 =====")
        qprint((
            f"Starting phase 7 from \n{twitter_rv_fpath} and "
            f"\n{uid2gender_fpath} \ninput files to {output_fpath} "
            "output file."))

        # edges_removed, edges_left = project_edge_list_to_user_intersection(
        #     twitter_rv_fpath=twitter_rv_fpath,
        #     uid2gender_fpath=uid2gender_fpath,
        #     output_fpath=output_fpath,
        # )

        # qprint((
        #     f"Removed {edges_removed:,} edges from the social graph; "
        #     f"{edges_left:,} remain. Graph dumped to {output_fpath}."))

        end = time.time()
        qprint((
            "Finished running phase 7 of the twikwak17 pipeline.\n"
            "Run duration: {}".format(seconds_to_duration_str(end - start))
        ))
    set_output_report_file_handle(None)
    create_timestamped_report_file_copy(output_report_fpath)
    return output_fpath
