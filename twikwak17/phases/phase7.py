"""Phase 7 of the twikwak17 dataset generation process."""

import re
import gc
import time
import gzip
# import zipfile
from contextlib import ExitStack


from twikwak17.shared import (
    qprint,
    seconds_to_duration_str,
    phase_output_report_fpath,
    set_output_report_file_handle,
    create_timestamped_report_file_copy,
    uid_to_gender_map_fpath_by_dpath,
    social_graph_fpath_by_dpath,
    graphml_fpath_by_dpath,
)


UID_TO_GENDER_REGEX = '(\d+) ([01])'


def uid_and_gender_from_line(line):
    """Breaks down a uid-to-gender line into user id and gender digit.

    Parameters
    ----------
    line : str
        A twikwak7 line of the format:
        "32e72983 1"

    Returns
    -------
    uid, gender : str, str
        A 2-tuple of the user id string and a one-character string of the
        user's predicted gender. E.g. ('837', '1')
    """
    if len(line) < 1:
        return None, None
    try:
        uid, gender = re.findall(UID_TO_GENDER_REGEX, line)[0]
    except IndexError:
        qprint("!!!")
        qprint(line)
    return uid, gender


UID_TO_UID_REGEX = '(\d+)[\t\s]+(\d+)'


def uids_from_edge_line(line):
    if len(line) < 1:
        return None, None
    uid, uid = re.findall(UID_TO_UID_REGEX, line)[0]
    return int(uid), int(uid)


GRAPHML_HEADER = ((
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<graphml xmlns="http://graphml.graphdrawing.org/xmlns"'
    '    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
    '    xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns'
    '     http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">\n'
    '  <key id="gender" for="node" attr.name="gender" attr.type="int"/>\n'
    '  <graph id="twikwak17" edgedefault="directed">\n'
))
GRAPHML_FOOTER = ((
    '  </graph>\n'
    '</graphml>\n'
))


def convert_twikwak17_to_graphml_format(
        uid2gender_fpath, social_graph_fpath, graphml_fpath):
    """Projects a user-to-user edge list to a given user intersection list.

    All edges between one (or two) users who cannot be found in the given user
    intersection list are removed in the new version of the user-to-user edge
    list file created.  d.

    Parameters
    ----------
    uid2gender_fpath : str
        The full qualified path to twikwak17's user-id-to-gender file.
    social_graph_fpath : str
        The full qualified path to twikwak17's social graph file.
    graphml_fpath : str
        The path to the designated output file.
    """
    qprint("Starting to convert twikwak17 to graphml format...")
    graph_sample_fpath = graphml_fpath[:-3]
    with ExitStack() as stack:
        out_f = stack.enter_context(gzip.open(graphml_fpath, 'wt+'))
        sample_f = stack.enter_context(open(graph_sample_fpath, 'wt+'))
        lines_read = 0
        lines_dumped = 0
        lines_to_dump = []
        sample_node_lines = False

        out_f.write(GRAPHML_HEADER)
        sample_f.write(GRAPHML_HEADER)
        qprint("graphml header dumped.")

        qprint("Starting to dump node information...")
        uid2gen_f = stack.enter_context(gzip.open(uid2gender_fpath, 'rt'))
        uid, gender = None, None
        uid2gender_line = uid2gen_f.readline()
        lines_read += 1

        while uid2gender_line:
            uid, gender = uid_and_gender_from_line(uid2gender_line)
            lines_to_dump.append((
                f'    <node id="{uid}">\n'
                f'      <data key="gender">{gender}</data>\n'
                f'    </node>\n'
            ))
            if len(lines_to_dump) >= 100000:
                lines = "".join(lines_to_dump)
                out_f.write(lines)
                if not sample_node_lines:
                    sample_f.write(lines)
                    sample_node_lines = True
                del lines
                lines_dumped += 100000
                lines_to_dump = None
                del lines_to_dump
                gc.collect()
                lines_to_dump = []
            uid2gender_line = uid2gen_f.readline()
            lines_read += 1
            if lines_read % 10000 == 0:
                qprint((
                    f"{lines_read:,} lines read|"
                    f"{lines_dumped:,} lines dumped. {uid} ~ {gender}."),
                    end="\r")
        if len(lines_to_dump) > 0:
            lines = "".join(lines_to_dump)
            out_f.write(lines)
            lines_dumped += len(lines_to_dump)
        nodes_dumped = lines_dumped
        qprint("Node information dumped.")

        qprint("Starting to dump edge information...")
        lines_read = 0
        lines_dumped = 0
        lines_to_dump = []
        sample_edge_lines = False
        edges_f = stack.enter_context(gzip.open(social_graph_fpath, 'rt'))
        edge_line = edges_f.readline()
        lines_read += 1

        while edge_line:
            uid1, uid2 = uids_from_edge_line(edge_line)
            lines_to_dump.append(
                f'    <edge source="{uid1}" target="{uid2}" />')
            if len(lines_to_dump) >= 100000:
                lines = "\n".join(lines_to_dump) + "\n"
                out_f.write(lines)
                if not sample_edge_lines:
                    sample_f.write(lines)
                    sample_edge_lines = True
                lines_dumped += 100000
                lines_to_dump = None
                del lines_to_dump
                gc.collect()
                lines_to_dump = []
            edge_line = edges_f.readline()
            lines_read += 1
            if lines_read % 10000 == 0:
                qprint((
                    f"{lines_read:,} lines read|"
                    f"{lines_dumped:,} lines dumped. {uid1} ~ {uid2}"),
                    end='\r')
        if len(lines_to_dump) > 0:
            lines = "\n".join(lines_to_dump) + "\n"
            out_f.write(lines)
            lines_dumped += len(lines_to_dump)
        qprint("Edge information dumped.")
        edges_dumped = lines_dumped

        out_f.write(GRAPHML_FOOTER)
        sample_f.write(GRAPHML_FOOTER)

    qprint("Conversion to graphml format complete.")
    return nodes_dumped, edges_dumped


def phase7(phase5_output_dpath, phase6_output_dpath, phase7_output_dpath):
    """Converts the twikwak17 dataset into the graphml format.

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
    uid2gender_fpath = uid_to_gender_map_fpath_by_dpath(phase5_output_dpath)
    social_graph_fpath = social_graph_fpath_by_dpath(phase6_output_dpath)
    graphml_fpath = graphml_fpath_by_dpath(phase7_output_dpath)
    output_report_fpath = phase_output_report_fpath(7, phase7_output_dpath)

    with open(output_report_fpath, 'wt+') as output_report_f:
        set_output_report_file_handle(output_report_f)
        qprint("\n\n====== PHASE 7 =====")
        qprint((
            f"Starting phase 7 from \n{uid2gender_fpath} and "
            f"\n{social_graph_fpath} \ninput files to {graphml_fpath} "
            "output file."))

        nodes_dumped, edges_dumped = convert_twikwak17_to_graphml_format(
            uid2gender_fpath=uid2gender_fpath,
            social_graph_fpath=social_graph_fpath,
            graphml_fpath=graphml_fpath,
        )

        qprint((
            f"{nodes_dumped:,} nodes and {edges_dumped:,} edges dumped into "
            f"graphml file at {graphml_fpath}."))

        end = time.time()
        qprint((
            "Finished running phase 7 of the twikwak17 pipeline.\n"
            "Run duration: {}".format(seconds_to_duration_str(end - start))
        ))
    set_output_report_file_handle(None)
    create_timestamped_report_file_copy(output_report_fpath)
    return graphml_fpath
