#!/usr/bin/env python
# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import argparse
import contextlib
import io
import itertools
import operator
import os
import sys
from hashlib import sha1

import jinja2
import signac
from define_status_test_project import TestProject
from test_project import redirect_stdout

import flow
import flow.environments
from flow.scheduling.fake_scheduler import FakeScheduler

PROJECT_NAME = "StatusTest"
ARCHIVE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "./status_reference_data.tar.gz")
)
PROJECT_DIRECTORY = "/home/user/project/"
MOCK_EXECUTABLE = "/usr/local/bin/python"


def init(project):
    """Initialize the data space for the given project."""

    for a in range(2, 4):
        for b in range(2):
            project.open_job(dict(a=a, b=b)).init()
            project.open_job(dict(a=dict(a, a), b=b)).init()


@contextlib.contextmanager
def get_masked_flowproject(p, environment=None):
    """Mock environment-dependent attributes and functions. Need to mock
    sys.executable before the FlowProject is instantiated, and then modify the
    root_directory and project_dir elements after creation."""
    try:
        old_executable = sys.executable
        sys.executable = MOCK_EXECUTABLE  # Not sure if this is necessary
        fp = TestProject.get_project(root=p.root_directory())
        if environment is not None:
            fp._environment = environment
        fp._entrypoint.setdefault("path", "generate_status_reference_data.py")
        fp.config.project_dir = PROJECT_DIRECTORY
        old_generate_id = flow.project.FlowGroup._generate_id

        def wrapped_generate_id(self, aggregate, *args, **kwargs):
            """Mock the root directory used for id generation.

            We need to generate consistent ids for all operations. This
            mocking has to happen within this method to avoid affecting other
            methods called during the test that access the project root directory.
            """
            old_root_directory = fp.root_directory
            fp.root_directory = lambda: PROJECT_DIRECTORY
            operation_id = old_generate_id(self, aggregate, *args, **kwargs)
            fp.root_directory = old_root_directory
            return operation_id

        flow.project.FlowGroup._generate_id = wrapped_generate_id
        yield fp

    finally:
        sys.executable = old_executable
        flow.project.FlowGroup._generate_id = old_generate_id


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

    with signac.TemporaryProject(name=PROJECT_NAME) as p:
        init(p)
        with get_masked_flowproject(p) as fp:
            # Here we set the appropriate executable for all the operations. This
            # is necessary as otherwise the default executable between submitting
            # and running could look different depending on the environment.
            for group in fp.groups.values():
                for op_key in group.operations:
                    if op_key in group.operation_directives:
                        group.operation_directives[op_key][
                            "executable"
                        ] = MOCK_EXECUTABLE
            for job in fp:
                with job:
                    kwargs = job.statepoint()
                    env = get_nested_attr(flow, kwargs["environment"])
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
                    parameters = kwargs["parameters"]
                    if "bundle" in parameters:
                        bundle = parameters.pop("bundle")
                        fn = "script_{}.sh".format("_".join(bundle))
                        tmp_out = io.TextIOWrapper(io.BytesIO(), sys.stdout.encoding)
                        with redirect_stdout(tmp_out):
                            try:
                                fp.submit(
                                    jobs=[job],
                                    names=bundle,
                                    pretend=True,
                                    force=True,
                                    bundle_size=len(bundle),
                                    **parameters,
                                )
                            except jinja2.TemplateError as e:
                                print(
                                    "ERROR:", e
                                )  # Shows template error in output script

                        # Filter out non-header lines
                        tmp_out.seek(0)
                        with open(fn, "w") as f:
                            with redirect_stdout(f):
                                print(tmp_out.read(), end="")
                    else:
                        for op in {**fp.operations, **fp.groups}:
                            if "partition" in parameters:
                                # Don't try to submit GPU operations to CPU partitions
                                # and vice versa.  We should be able to relax this
                                # requirement if we make our error checking more
                                # consistent.
                                if operator.xor(
                                    "gpu" in parameters["partition"].lower(),
                                    "gpu" in op.lower(),
                                ):
                                    continue
                            fn = f"script_{op}.sh"
                            tmp_out = io.TextIOWrapper(
                                io.BytesIO(), sys.stdout.encoding
                            )
                            with redirect_stdout(tmp_out):
                                try:
                                    fp.submit(
                                        jobs=[job],
                                        names=[op],
                                        pretend=True,
                                        force=True,
                                        **parameters,
                                    )
                                except jinja2.TemplateError as e:
                                    print(
                                        "ERROR:", e
                                    )  # Shows template error in output script

                            # Filter out non-header lines and the job-name line
                            tmp_out.seek(0)
                            with open(fn, "w") as f:
                                with redirect_stdout(f):
                                    print(tmp_out.read(), end="")

            # For compactness, we move the output into an ARCHIVE_DIR then delete the original data.
            fp.export_to(target=ARCHIVE_DIR)


if __name__ == "__main__":
    flow.FlowProject._store_bundled = _store_bundled

    parser = argparse.ArgumentParser(
        description="Generate reference submission scripts for various environments"
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Recreate the data space even if the ARCHIVE_DIR already exists",
    )
    main(parser.parse_args())
