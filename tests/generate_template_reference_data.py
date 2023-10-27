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
from define_template_test_project import TestProject
from test_project import redirect_stdout

import flow
import flow.environments
from flow.scheduling.fake_scheduler import FakeScheduler

# Define a consistent submission name so that we can test that job names are
# being correctly generated.
ARCHIVE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "./template_reference_data.tar.gz")
)
PROJECT_DIRECTORY = '/home/user/path with spaces and "quotes" and \\backslashes/'
MOCK_EXECUTABLE = "/usr/local/bin/python"


def cartesian(**kwargs):
    """Generate a set of statepoint dictionaries from a dictionary of the form
    {key1: [list of values], key2: [list of values]...}"""
    for combo in itertools.product(*kwargs.values()):
        yield dict(zip(kwargs.keys(), combo))


def get_nested_attr(obj, attr, default=None):
    """Get nested attributes of an object."""
    attrs = attr.split(".")
    for a in attrs:
        try:
            obj = getattr(obj, a)
        except AttributeError:
            if default:
                return default
            else:
                raise
    return obj


def in_line(patterns, line):
    """Check if any of the strings in the list patterns are in the line"""
    return any([p in line for p in patterns])


def init(project):
    """Initialize the data space for the given project."""
    # This object is a dictionary whose keys are environments. Each environment
    # is associated with a list of dictionaries, where each dictionary contains
    # a set of parameters that need to be tested together. For instance
    # bundling and parallelism must exist in the same test. The goal is to
    # construct a minimal covering set of all test cases.
    environments = {
        "environment.StandardEnvironment": [],
        "environments.xsede.Stampede2Environment": [
            {
                "partition": ["skx-normal"],
            },
            {
                "partition": ["skx-normal"],
                "parallel": [False, True],
                "bundle": [["mpi_op", "mpi_op"], ["omp_op", "omp_op"]],
            },
        ],
        "environments.xsede.Bridges2Environment": [
            {
                "partition": ["RM", "RM-shared", "GPU", "GPU-shared"],
            },
            {
                "partition": ["RM"],
                "parallel": [False, True],
                "bundle": [["mpi_op", "omp_op"]],
            },
        ],
        "environments.umich.GreatLakesEnvironment": [
            {
                "partition": ["standard", "gpu"],
            },
            {
                "parallel": [False, True],
                "bundle": [["mpi_op", "omp_op"]],
            },
        ],
        "environments.incite.SummitEnvironment": [
            {},
            {
                "parallel": [False, True],
                "bundle": [["mpi_op", "omp_op"]],
            },
        ],
        "environments.incite.AndesEnvironment": [
            {
                "partition": ["batch", "gpu"],
            },
            {
                "partition": ["batch"],
                "parallel": [False, True],
                "bundle": [["mpi_op", "omp_op"]],
            },
        ],
        "environments.umn.MangiEnvironment": [
            {},
            {
                "parallel": [False, True],
                "bundle": [["mpi_op", "omp_op"]],
            },
        ],
        "environments.xsede.ExpanseEnvironment": [
            {
                "partition": ["compute", "shared", "gpu", "gpu-shared", "large-shared"],
            },
            {
                "partition": ["compute"],
                "parallel": [False, True],
                "bundle": [["mpi_op", "omp_op"]],
            },
        ],
        "environments.drexel.PicotteEnvironment": [
            {
                "partition": ["def", "gpu"],
            },
            {
                "partition": ["def"],
                "parallel": [False, True],
                "bundle": [["mpi_op", "omp_op"]],
            },
        ],
        "environments.xsede.DeltaEnvironment": [
            {
                "partition": ["cpu", "gpuA40x4", "gpuA100x4"],
            },
            {
                "partition": ["cpu"],
                "parallel": [False, True],
                "bundle": [["mpi_op", "omp_op"]],
            },
        ],
        "environments.incite.CrusherEnvironment": [
            {},
            {
                "parallel": [False, True],
                "bundle": [["mpi_op", "omp_op"]],
            },
        ],
        # Frontier cannot use partitions as logic requires gpu
        # in the name of partitions that are gpu nodes.
        "environments.incite.FrontierEnvironment": [
            {},
            {
                "parallel": [False, True],
                "bundle": [["mpi_op", "omp_op"]],
            },
        ],
        "environments.purdue.AnvilEnvironment": [
            {
                "partition": ["debug", "gpu-debug", "wholenode", "wide", "shared", "highmem", "gpu"],
            },
            {
                "partition": ["wholenode"],
                "parallel": [False, True],
                "bundle": [["mpi_op", "omp_op"]],
            },
        ],
    }

    for environment, parameter_combinations in environments.items():
        for parameter_sets in parameter_combinations:
            params = cartesian(**parameter_sets)
            for param in params:
                sp = dict(environment=environment, parameters=param)
                project.open_job(sp).init()


# Mock the bundle storing to avoid needing to make a file
def _store_bundled(self, operations):
    if len(operations) == 1:
        return operations[0].id
    else:
        h = ".".join(op.id for op in operations)
        bid = "{}/bundle/{}".format(
            self.__class__.__name__, sha1(h.encode("utf-8")).hexdigest()
        )
        return bid


@contextlib.contextmanager
def get_masked_flowproject(p, environment=None):
    """Mock environment-dependent attributes and functions. Need to mock
    sys.executable before the FlowProject is instantiated, and then modify the
    path after creation."""
    try:
        old_executable = sys.executable
        sys.executable = MOCK_EXECUTABLE
        fp = TestProject.get_project(p.path)
        if environment is not None:
            fp._environment = environment
        fp._entrypoint.setdefault("path", "generate_template_reference_data.py")
        old_generate_id = flow.project.FlowGroup._generate_id
        old_standard_template_context = flow.FlowProject._get_standard_template_context

        def mocked_generate_id(self, aggregate, *args, **kwargs):
            """Mock the root directory used for id generation.

            We need to generate consistent ids for all operations. This
            mocking has to happen within this method to avoid affecting other
            methods called during the test that access the project root directory.
            """
            old_path = fp.path
            fp._path = PROJECT_DIRECTORY
            operation_id = old_generate_id(self, aggregate, *args, **kwargs)
            fp._path = old_path
            return operation_id

        def mocked_template_context(self):
            context = old_standard_template_context(self)
            context["project"]._path = PROJECT_DIRECTORY
            return context

        flow.project.FlowGroup._generate_id = mocked_generate_id
        flow.FlowProject._get_standard_template_context = mocked_template_context
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

    with signac.TemporaryProject() as p:
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
