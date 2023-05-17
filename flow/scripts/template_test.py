"""Implement the test-workflow flow CLI subcommand."""
import errno
import logging
import os
import string
import subprocess

import jinja2

logger = logging.getLogger(__name__)


def main_test_workflow(args):
    """Create a workspace with a workflow to test submission to schedulers.

    This is designed for testing HPC clusters with templates where CI/automated  testing that is
    difficult.
    """
    # Create project.py
    prompt = "Number of {} per node."
    ngpus = args.ngpus if args.ngpus is not None else int(input(prompt.format("GPU")))
    ncpus = args.ncpus if args.ncpus is not None else int(input(prompt.format("CPU")))
    context = {"ncpus": ncpus, "ngpus": ngpus}
    jinja_env = jinja2.Environment(loader=jinja2.PackageLoader("flow"))
    project_template = jinja_env.get_template("project.py")
    project_content = project_template.render(**context).strip(string.whitespace)

    try:
        with open("project.py", "x") as fh:
            fh.write(project_content)
    except OSError as error:
        if error.errno == errno.EEXIST:
            logger.error(
                "Error while trying to create custom template. Delete 'project.py' "
                "first and rerun command."
            )
        else:
            logger.error(f"Error while trying to create testing project: '{error}'.")
        raise

    # create init.py
    init_content = jinja_env.get_template("init.py").render(num_jobs=args.num_jobs)
    try:
        with open("init.py", "x") as fh:
            fh.write(init_content)
    except OSError as error:
        if error.errno == errno.EEXIST:
            logger.error(
                "Error while trying to create custom template. Delete 'init.py' "
                "first and rerun command."
            )
        else:
            logger.error(f"Error while trying to create testing project: '{error}'.")
        os.remove("project.py")
        raise

    # Run init.py
    try:
        subprocess.run(["python3", "init.py"])
    except subprocess.SubprocessError:
        logger.warning(
            "Error attempting to run init.py. Run manually to initialize workspace."
        )


def test_workflow_parser(subparser):
    """Add parser arguments to the test-workflow subpaser."""
    subparser.add_argument(
        "--num_gpus",
        "-g",
        nargs=1,
        type=int,
        default=None,
        help="Specify the number of GPUs per node for the desired GPU partition."
        "Set to 0 to specify no GPUs.",
    )
    subparser.add_argument(
        "--num_cpus",
        "-c",
        nargs=1,
        type=int,
        default=None,
        help="Specify the number of CPUs per node for the desired CPU partition.",
    )
    subparser.add_argument(
        "--num_jobs",
        "-n",
        nargs=1,
        type=int,
        default=10,
        help="Specify the number of jobs to initialize. Defaults to 10.",
    )

    subparser.set_defaults(func=main_test_workflow)
