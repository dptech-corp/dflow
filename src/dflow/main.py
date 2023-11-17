import argparse
import datetime
import json
from typing import List, Optional

from dflow import (S3Artifact, Secret, Workflow, config, download_artifact,
                   gen_code, query_workflows, upload_artifact)


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
    parser_list.add_argument(
        "-l",
        "--label",
        type=str,
        default=None,
        help="query by labels",
    )

    parser_get = subparsers.add_parser(
        "get",
        help="Get a workflow",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_get.add_argument("ID", help="the workflow ID.")

    parser_getsteps = subparsers.add_parser(
        "getsteps",
        help="Get steps from a workflow",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_getsteps.add_argument("ID", help="the workflow ID.")
    parser_getsteps.add_argument(
        "-n",
        "--name",
        type=str,
        default=None,
        help="query by name",
    )
    parser_getsteps.add_argument(
        "-k",
        "--key",
        type=str,
        default=None,
        help="query by key",
    )
    parser_getsteps.add_argument(
        "-p",
        "--phase",
        type=str,
        default=None,
        help="query by phase",
    )
    parser_getsteps.add_argument(
        "-i",
        "--id",
        type=str,
        default=None,
        help="query by ID",
    )
    parser_getsteps.add_argument(
        "-t",
        "--type",
        type=str,
        default=None,
        help="query by type",
    )

    parser_getkeys = subparsers.add_parser(
        "getkeys",
        help="Get keys of steps from a workflow",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_getkeys.add_argument("ID", help="the workflow ID.")

    parser_delete = subparsers.add_parser(
        "delete",
        help="Delete a workflow",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_delete.add_argument("ID", help="the workflow ID.")

    parser_resubmit = subparsers.add_parser(
        "resubmit",
        help="Resubmit a workflow",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_resubmit.add_argument("ID", help="the workflow ID.")

    parser_resume = subparsers.add_parser(
        "resume",
        help="Resume a workflow",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_resume.add_argument("ID", help="the workflow ID.")

    parser_retry = subparsers.add_parser(
        "retry",
        help="Retry a workflow",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_retry.add_argument("ID", help="the workflow ID.")
    parser_retry.add_argument(
        "-s",
        "--step",
        type=str,
        default=None,
        help="retry a step in a running workflow with step ID (experimental)",
    )

    parser_stop = subparsers.add_parser(
        "stop",
        help="Stop a workflow",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_stop.add_argument("ID", help="the workflow ID.")

    parser_suspend = subparsers.add_parser(
        "suspend",
        help="Suspend a workflow",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_suspend.add_argument("ID", help="the workflow ID.")

    parser_terminate = subparsers.add_parser(
        "terminate",
        help="Terminate a workflow",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_terminate.add_argument("ID", help="the workflow ID.")

    parser_wait = subparsers.add_parser(
        "wait",
        help="Wait a workflow to complete",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_wait.add_argument("ID", help="the workflow ID.")
    parser_wait.add_argument(
        "-i",
        "--interval",
        type=int,
        default=1,
        help="time interval between two queries",
    )

    parser_download = subparsers.add_parser(
        "download",
        help="Download an artifact",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_download.add_argument(
        "-k",
        "--key",
        type=str,
        default=None,
        help="storage key of the artifact",
    )
    parser_download.add_argument(
        "-u",
        "--urn",
        type=str,
        default=None,
        help="URN of the artifact",
    )
    parser_download.add_argument(
        "-p",
        "--path",
        type=str,
        default=".",
        help="the path to which the artifact will be downloaded",
    )

    parser_upload = subparsers.add_parser(
        "upload",
        help="Upload an artifact",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_upload.add_argument(
        "-p",
        "--path",
        type=str,
        required=True,
        help="path of the local file(s)",
    )
    parser_upload.add_argument(
        "-n",
        "--namespace",
        type=str,
        default=None,
        help="namespace for registering dataset",
    )
    parser_upload.add_argument(
        "-d",
        "--name",
        type=str,
        default=None,
        help="dataset name for registering dataset",
    )
    parser_upload.add_argument(
        "-s",
        "--description",
        type=str,
        default=None,
        help="description for registering dataset",
    )
    parser_upload.add_argument(
        "-t",
        "--tag",
        type=str,
        default=None,
        help="tags for registering dataset",
    )
    parser_upload.add_argument(
        "-r",
        "--property",
        type=str,
        default=None,
        help="properties for registering dataset",
    )

    parser_submit = subparsers.add_parser(
        "submit",
        help="Submit a workflow from a YAML file",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_submit.add_argument("FILE", help="the YAML file")
    parser_submit.add_argument(
        "-d",
        "--detach",
        action="store_true",
        help="detach mode for running workflow",
    )
    parser_submit.add_argument(
        "-n",
        "--name",
        type=str,
        default=None,
        help="workflow ID",
    )

    parser_create = subparsers.add_parser(
        "create",
        help="Create a resource",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    create_subparsers = parser_create.add_subparsers(
        title="Valid resources", dest="resource")
    parser_secret = create_subparsers.add_parser(
        "secret",
        help="Create a secret",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_secret.add_argument("value", help="secret value")
    parser_secret.add_argument(
        "-n",
        "--name",
        type=str,
        help="name of the secret",
    )
    parser_secret.add_argument(
        "-k",
        "--key",
        type=str,
        default="secret",
        help="key in the secret",
    )

    parser_codegen = subparsers.add_parser(
        "codegen",
        help="Generate code from graph",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_codegen.add_argument("GRAPH", help="the graph JSON file.")
    parser_codegen.add_argument("-o", "--output", type=str,
                                help="File path of the generated code")
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
            if len(str(s)) > maxlen[i]:
                maxlen[i] = len(str(s))
    for row in t:
        for i, s in enumerate(row):
            print(str(s) + " " * (maxlen[i]-len(str(s))+3), end="")
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
        if args.label is not None:
            labels = {}
            for label in args.label.split(","):
                key, value = label.split("=")
                labels[key] = value
        else:
            labels = None
        wfs = query_workflows(labels=labels)
        t = [["NAME", "STATUS", "AGE", "DURATION"]]
        for wf in wfs:
            tc = datetime.datetime.strptime(wf.metadata.creationTimestamp,
                                            "%Y-%m-%dT%H:%M:%SZ")
            age = format_time_delta(datetime.datetime.now() - tc)
            dur = format_time_delta(wf.get_duration())
            t.append([wf.id, wf.status.phase, age, dur])
        format_print_table(t)
    elif args.command == "get":
        wf_id = args.ID
        wf = Workflow(id=wf_id)
        info = wf.query()
        t = []
        t.append(["Name:", info.id])
        t.append(["Status:", info.status.phase])
        t.append(["Created:", info.metadata.creationTimestamp])
        t.append(["Started:", info.status.startedAt])
        t.append(["Finished:", info.status.finishedAt])
        t.append(["Duration", format_time_delta(info.get_duration())])
        t.append(["Progress:", info.status.progress])
        format_print_table(t)
        print()
        steps = info.get_step()
        t = [["STEP", "ID", "KEY", "TYPE", "PHASE", "DURATION"]]
        for step in steps:
            if step.type in ["StepGroup"]:
                continue
            key = step.key if step.key is not None else ""
            dur = format_time_delta(step.get_duration())
            t.append([step.displayName, step.id, key, step.type, step.phase,
                      dur])
        format_print_table(t)
    elif args.command == "getsteps":
        wf_id = args.ID
        name = args.name
        key = args.key
        phase = args.phase
        id = args.id
        type = args.type
        if name is not None:
            name = name.split(",")
        if key is not None:
            key = key.split(",")
        if phase is not None:
            phase = phase.split(",")
        if id is not None:
            id = id.split(",")
        if type is not None:
            type = type.split(",")
        wf = Workflow(id=wf_id)
        if key is not None:
            steps = wf.query_step_by_key(key, name, phase, id, type)
        else:
            steps = wf.query_step(name, key, phase, id, type)
        for step in steps:
            if step.type in ["StepGroup"]:
                continue
            key = step.key if step.key is not None else ""
            dur = format_time_delta(step.get_duration())
            t = []
            t.append(["Step:", step.displayName])
            t.append(["ID:", step.id])
            t.append(["Key:", key])
            t.append(["Type:", step.type])
            t.append(["Phase:", step.phase])
            format_print_table(t)
            if hasattr(step, "outputs"):
                if hasattr(step.outputs, "parameters"):
                    print("Output parameters:")
                    for name, par in step.outputs.parameters.items():
                        if name[:6] == "dflow_":
                            continue
                        print("%s: %s" % (name, par.value))
                    print()
                if hasattr(step.outputs, "artifacts"):
                    print("Output artifacts:")
                    for name, art in step.outputs.artifacts.items():
                        if name[:6] == "dflow_" or name == "main-logs":
                            continue
                        key = ""
                        if hasattr(art, "s3"):
                            key = art.s3.key
                        elif hasattr(art, "oss"):
                            key = art.oss.key
                        print("%s: %s" % (name, key))
                    print()
            print()
    elif args.command == "getkeys":
        wf_id = args.ID
        wf = Workflow(id=wf_id)
        keys = wf.query_keys_of_steps()
        print("\n".join(keys))
    elif args.command == "delete":
        wf_id = args.ID
        wf = Workflow(id=wf_id)
        wf.delete()
    elif args.command == "resubmit":
        wf_id = args.ID
        wf = Workflow(id=wf_id)
        wf.resubmit()
    elif args.command == "resume":
        wf_id = args.ID
        wf = Workflow(id=wf_id)
        wf.resume()
    elif args.command == "retry":
        wf_id = args.ID
        wf = Workflow(id=wf_id)
        if args.step is not None:
            wf.retry_steps(args.step.split(","))
        else:
            wf.retry()
    elif args.command == "stop":
        wf_id = args.ID
        wf = Workflow(id=wf_id)
        wf.stop()
    elif args.command == "suspend":
        wf_id = args.ID
        wf = Workflow(id=wf_id)
        wf.suspend()
    elif args.command == "terminate":
        wf_id = args.ID
        wf = Workflow(id=wf_id)
        wf.terminate()
    elif args.command == "wait":
        wf_id = args.ID
        wf = Workflow(id=wf_id)
        wf.wait(interval=args.interval)
    elif args.command == "download":
        assert args.key is not None or args.urn is not None, \
            "one of -k/--key and -u/--urn must be specified"
        art = S3Artifact(key=args.key, urn=args.urn)
        path = download_artifact(art, path=args.path)
        print("Downloaded artifact to %s" % path)
    elif args.command == "upload":
        if "=" in args.path:
            path = {}
            for p in args.path.split(","):
                k, v = p.split("=")
                path[k] = v
        else:
            path = args.path.split(",")
        tags = None
        if args.tag is not None:
            tags = args.tag.split(",")
        properties = None
        if args.property is not None:
            properties = {}
            for p in args.property.split(","):
                k, v = p.split("=")
                properties[k] = v
        art = upload_artifact(
            path, namespace=args.namespace, dataset_name=args.name,
            description=args.description, tags=tags, properties=properties)
        print("Storage key: %s" % art.key)
        if art.urn:
            print("Dataset URN: %s" % art.urn)
    elif args.command == "submit":
        with open(args.FILE, "r") as f:
            wf = Workflow.from_yaml(f.read())
        if args.name is not None:
            wf.id = args.name
        if args.detach:
            config["detach"] = True
        wf.submit()
    elif args.command == "create":
        if args.resource == "secret":
            s = Secret(args.value, args.name, args.key)
            print("Secret (name: %s, key: %s) created" % (s.secret_name,
                                                          s.secret_key))
    elif args.command == "codegen":
        with open(args.GRAPH, "r") as f:
            graph = json.load(f)
        code = gen_code(graph)
        if args.output is None:
            print(code)
        else:
            with open(args.output, "w") as f:
                f.write(code)


if __name__ == "__main__":
    main()
