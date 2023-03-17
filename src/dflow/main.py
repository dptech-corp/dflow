import argparse
import datetime
from typing import List, Optional

from dflow import query_workflows


def main_parser():
    parser = argparse.ArgumentParser(
        description="dflow: a Python framework for constructing scientific "
        "computing workflows",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(
        title="Valid subcommands", dest="command")

    parser_list = subparsers.add_parser(
        "list",
        help="List workflows",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    return parser


def parse_args(args: Optional[List[str]] = None):
    """Commandline options argument parsing.

    Parameters
    ----------
    args : List[str]
        list of command line arguments, main purpose is testing default option
        None takes arguments from sys.argv
    """
    parser = main_parser()
    parsed_args = parser.parse_args(args=args)
    if parsed_args.command is None:
        parser.print_help()
    return parsed_args


def format_print_table(t: List[List[str]]):
    ncol = len(t[0])
    maxlen = [0] * ncol
    for row in t:
        for i, s in enumerate(row):
            if len(s) > maxlen[i]:
                maxlen[i] = len(s)
    for row in t:
        for i, s in enumerate(row):
            print(s + " " * (maxlen[i]-len(s)+3), end="")
        print()


def format_time_delta(td: datetime.timedelta) -> str:
    if td.days > 0:
        return "%dd" % td.days
    elif td.seconds >= 3600:
        return "%dh" % (td.seconds // 3600)
    else:
        return "%ds" % td.seconds


def main():
    args = parse_args()

    if args.command == "list":
        wfs = query_workflows()
        t = [["NAME", "STATUS", "AGE", "DURATION"]]
        for wf in wfs:
            tc = datetime.datetime.strptime(wf.metadata.creationTimestamp,
                                            "%Y-%m-%dT%H:%M:%SZ")
            age = format_time_delta(datetime.datetime.now() - tc)
            if wf.status.startedAt is None:
                dur = datetime.timedelta()
            else:
                ts = datetime.datetime.strptime(wf.status.startedAt,
                                                "%Y-%m-%dT%H:%M:%SZ")
                if wf.status.finishedAt is None:
                    tf = datetime.datetime.now()
                else:
                    tf = datetime.datetime.strptime(wf.status.finishedAt,
                                                    "%Y-%m-%dT%H:%M:%SZ")
                dur = format_time_delta(tf - ts)
            t.append([wf.id, wf.status.phase, age, dur])
        format_print_table(t)


if __name__ == "__main__":
    main()
