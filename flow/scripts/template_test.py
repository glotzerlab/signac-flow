"""Implement the test-workflow flow CLI subcommand."""
import errno
import logging
import os
import stat
import string
import subprocess

import jinja2

logger = logging.getLogger(__name__)


def _make_executable(fn):
    mode = stat.S_IMODE(os.stat(fn).st_mode)
    os.chmod(fn, mode | stat.S_IXUSR)


def _cleanup():
    for fn in ("init.py", "project.py"):
        if os.path.exists(fn):
            os.remove(fn)


def _create_file(fn, content):
    try:
        with open(fn, "x") as fh:
            fh.write(content)
    except OSError as error:
        if error.errno == errno.EEXIST:
            logger.error(
                f"Error while trying to create custom template. Delete '{fn}' "
                f"first and rerun command."
            )
        else:
            logger.error(f"Error while trying to create testing project: '{error}'.")
        _cleanup()
        raise
    _make_executable(fn)


def main_test_workflow(args):
    """Create a workspace with a workflow to test submission to schedulers.

    This is designed for testing HPC clusters with templates where CI/automated  testing that is
    difficult.
    """
    # Create project.py
    prompt = "Number of {} per node: "
    num_cpus = (
        args.num_cpus[0]
        if args.num_cpus is not None
        else int(input(prompt.format("CPU")))
    )
    num_gpus = (
        args.num_gpus[0]
        if args.num_gpus is not None
        else int(input(prompt.format("GPU")))
    )
    context = {"num_cpus": num_cpus, "num_gpus": num_gpus}
    jinja_env = jinja2.Environment(loader=jinja2.PackageLoader("flow", "templates"))
    project_template = jinja_env.get_template("project_test_environment.pyt")
    project_content = project_template.render(**context).strip(string.whitespace)
    _create_file("project.py", project_content)

    # create init.py
    init_content = jinja_env.get_template("init_test_environment.pyt").render(
        num_jobs=args.num_jobs[0]
    )
    _create_file("init.py", init_content)

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
        default=(10,),
        help="Specify the number of jobs to initialize. Defaults to 10.",
    )

    subparser.set_defaults(func=main_test_workflow)
