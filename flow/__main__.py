# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""The main command line interface for signac-flow.

The interface is accessible via the `flow` command and allows users to
initialize FlowProject class definitions directly from the command line.

Execute `flow --help` for more information.
"""
import argparse
import errno
import logging
import os
import sys

import jinja2
from signac import get_project, init_project

from . import __version__, environment, template

logger = logging.getLogger(__name__)


def main_init(args):
    """Initialize a FlowProject from a template.

    The available templates are defined in the template module.
    """
    if not args.alias.isidentifier():
        raise ValueError(
            "The alias '{}' is not a valid Python identifier and can therefore "
            "not be used as a FlowProject alias.".format(args.alias)
        )
    try:
        get_project()
    except LookupError:
        init_project(name=args.alias)
        print(
            "Initialized signac project with name '{}' in "
            "current directory.".format(args.alias),
            file=sys.stderr,
        )
    try:
        return template.init(alias=args.alias, template=args.template)
    except OSError as error:
        raise RuntimeError(
            f"Error occurred while trying to initialize a flow project: {error}"
        )


def main_template(args):
    """Print out general information about template subcommand."""
    print("flow template does nothing. Use of the available subcommands [create,].")


def main_template_create(args):
    """Create custom template based on a specified or detected environment."""
    if args.name is None:
        env = environment.get_environment()
        if env is environment.StandardEnvironment:
            extend_template = "base_script.sh"
        else:
            extend_template = env.template
    else:
        extend_template = args.name

    project = get_project()
    os.makedirs(project.fn("templates"), exist_ok=True)

    # grab and render custom template
    jinja_env = jinja2.Environment(loader=jinja2.PackageLoader("flow"))
    custom_template = jinja_env.get_template("custom.sh")
    new_template = custom_template.render(extend_template=extend_template)

    try:
        with open(project.fn(os.path.join("templates", "script.sh")), "x") as fh:
            fh.write(new_template)
    except OSError as error:
        if error.errno == errno.EEXIST:
            logger.error(
                "The file 'templates/script.sh' already exists. Delete "
                "'templates/script.sh' first and rerun command."
            )
        else:
            logger.error(f"Error while trying to create custom template: '{error}'.")


def main():
    """Define the 'flow' command line interface.

    This function defines the main 'flow' command line interface which can be used
    to initialize FlowProject modules from different templates as well as print the
    version of the installed signac-flow package.
    """
    parser = argparse.ArgumentParser(
        description="flow provides the basic components to set up workflows for "
        "projects as part of the signac framework."
    )
    parser.add_argument(
        "--debug", action="store_true", help="Show traceback on error for debugging."
    )
    parser.add_argument(
        "--version", action="store_true", help="Display the version number and exit."
    )

    subparsers = parser.add_subparsers()

    # the flow init command
    parser_init = subparsers.add_parser(
        "init", help="Initialize a signac-flow project."
    )
    parser_init.set_defaults(func=main_init)
    parser_init.add_argument(
        "alias",
        type=str,
        nargs="?",
        default="project",
        help="Name of the FlowProject module to initialize. "
        "This name will also be used to initialize a signac project if "
        "no signac project was initialized prior to calling 'init'. "
        "Default value: 'project'.",
    )
    parser_init.add_argument(
        "-t",
        "--template",
        type=str,
        choices=tuple(sorted(template.TEMPLATES)),
        default="minimal",
        help="Specify a template to use. Default value: 'minimal'.",
    )

    # the flow template command
    parser_template = subparsers.add_parser(
        "template", help="Create and manage custom user templates."
    )
    template_subparsers = parser_template.add_subparsers()
    parser_template.set_defaults(func=main_template)

    # flow template create command
    parser_template_create = template_subparsers.add_parser(
        "create",
        help="Create a new custom template based on the detected or selected environment",
    )
    parser_template_create.add_argument(
        "name",
        nargs="?",
        type=str,
        default=None,
        help="Optional specify a template to use as the base template to "
        "extend. If not specified, the currently detected environment's template is "
        "used. ",
    )

    parser_template_create.set_defaults(func=main_template_create)

    if "--version" in sys.argv:
        print("signac-flow", __version__)
        sys.exit(0)

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    if not hasattr(args, "func"):
        parser.print_usage()
        sys.exit(2)
    try:
        args.func(args)
    except KeyboardInterrupt:
        sys.stderr.write("\n")
        sys.stderr.write("Interrupted.\n")
        if args.debug:
            raise
        sys.exit(1)
    except Exception as error:
        sys.stderr.write(f"{str(error)}\n")
        if args.debug:
            raise
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
