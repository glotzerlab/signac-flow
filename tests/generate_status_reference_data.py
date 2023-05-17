#!/usr/bin/env python
# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import argparse
import os

import signac
from define_status_test_project import _TestProject

import flow
import flow.environments
from flow.scheduling.fake_scheduler import FakeScheduler

ARCHIVE_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "./status_reference_data.tar.gz")
)


def init(project):
    """Initialize the data space for the given project."""

    for a in range(2, 4):
        for b in range(2):
            job = project.open_job(dict(a=a, b=b)).init()
            job.doc.a = a
            job = project.open_job(dict(a=dict(a=a), b=b)).init()
            job.doc.b = b


def init_status_options(project):
    options = [
        {},  # default options
        {"overview": False},  # --no-overview
        {"overview_max_lines": 2},  # --overview-max-lines 2
        {"detailed": True},  # -d, --detailed
        {"parameters": ["a"]},  # -p a, --parameters a
        {"parameters": ["sp.a"]},  # -p sp.a, --parameters sp.a
        {"parameters": ["doc.a"]},  # -p doc.a, --parameters doc.a
        {"param_max_width": 1},  # --param-max-width 1
        {"expand": True},  # -e, --expand
        {"all_ops": True},  # -a, --all-operations
        {"only_incomplete": True},  # --only-incomplete-operations
        {"dump_json": True},  # --json
        {"unroll": False},  # --stack
        {"compact": True},  # -1, --one-line
        {"pretty": True},  # --pretty
        {"eligible_jobs_max_lines": 2},  # --eligible-jobs-max-lines 2
        {"output_format": "markdown"},  # --output-format markdown
        {"output_format": "html"},  # --output-format html
        {"operation": ["op1", "op2"]},  # -o op1 op2, --operation op1 op2
    ]

    for sp in options:
        project.open_job(sp).init()


def main(args):
    # If the ARCHIVE_PATH already exists, only recreate if forced.
    if os.path.exists(ARCHIVE_PATH):
        if args.force:
            print(f"Removing existing archive '{ARCHIVE_PATH}'.")
            os.unlink(ARCHIVE_PATH)
        else:
            print(
                f"Archive '{ARCHIVE_PATH}' already exists, exiting. "
                "Use `-f/--force` to overwrite."
            )
            return

    with signac.TemporaryProject() as p, signac.TemporaryProject() as status_pr:
        init(p)
        init_status_options(status_pr)
        fp = _TestProject.get_project(path=p.path)
        env = flow.environment.TestEnvironment
        # We need to set the scheduler manually. The FakeScheduler
        # won't try to call any cluster executable (e.g. squeue)
        # associated with the real schedulers used on supported
        # clusters. Otherwise status check would fail when
        # attempting to determine what jobs exist on the scheduler.
        env.scheduler_type = FakeScheduler
        fp._environment = env
        for job in status_pr:
            kwargs = job.statepoint()
            with open(job.fn("status.txt"), "w") as status_file:
                fp.print_status(**kwargs, file=status_file)

        # For compactness, we move the output into an ARCHIVE_PATH then delete the original data.
        status_pr.export_to(target=ARCHIVE_PATH, path=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate reference status output")
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Recreate the data space even if the ARCHIVE_PATH already exists",
    )
    main(parser.parse_args())
