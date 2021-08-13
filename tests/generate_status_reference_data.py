#!/usr/bin/env python
# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import argparse
import os

import signac
from define_status_test_project import _TestProject
from test_project import redirect_stdout

import flow
import flow.environments
from flow.scheduling.fake_scheduler import FakeScheduler

PROJECT_NAME = "StatusTest"
ARCHIVE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "./status_reference_data.tar.gz")
)


def init(project):
    """Initialize the data space for the given project."""

    for a in range(2, 4):
        for b in range(2):
            project.open_job(dict(a=a, b=b)).init()
            project.open_job(dict(a=dict(a=a), b=b)).init()


def init_status_options(project):

    options = [{}, {"detailed": True}, {"parameters": ["a"]}, {"expand": True}]

    for sp in options:
        project.open_job(sp).init()


def main(args):
    # If the ARCHIVE_DIR already exists, only recreate if forced.
    if os.path.exists(ARCHIVE_DIR):
        if args.force:
            print(f"Removing existing archive '{ARCHIVE_DIR}'.")
            os.unlink(ARCHIVE_DIR)
        else:
            print(
                "Archive '{}' already exists, exiting. "
                "Use `-f/--force` to overwrite.".format(ARCHIVE_DIR)
            )
            return

    with signac.TemporaryProject(name=PROJECT_NAME) as p, signac.TemporaryProject(
        name="StatusProject"
    ) as status_pr:
        init(p)
        init_status_options(status_pr)
        fp = _TestProject.get_project(root=p.root_directory())
        env = flow.environment.TestEnvironment
        # We need to set the scheduler manually. The FakeScheduler
        # is used for two reasons. First, the FakeScheduler prints
        # scripts to screen on submission and we can capture that
        # output. Second, the FakeScheduler won't try to call any
        # cluster executable (e.g. squeue) associated with the real
        # schedulers used on supported clusters. Otherwise
        # submission would fail when attempting to determine what
        # jobs already exist on the scheduler.
        env.scheduler_type = FakeScheduler
        fp._environment = env
        for job in status_pr:
            with job:
                kwargs = job.statepoint()
                fn = job.fn("status.txt")

                with open(fn, "w") as f:
                    with redirect_stdout(f):
                        fp.print_status(**kwargs)

        # For compactness, we move the output into an ARCHIVE_DIR then delete the original data.
        status_pr.export_to(target=ARCHIVE_DIR, path=False)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Generate reference status output")
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Recreate the data space even if the ARCHIVE_DIR already exists",
    )
    main(parser.parse_args())
