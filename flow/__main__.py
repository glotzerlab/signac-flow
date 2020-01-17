# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""This module defines the main command line interface for signac-flow.

The interface is accessible via the `flow` command and allows users to
initialize FlowProject class definitions directly from the command line.

Execute `flow --help` for more information.
"""
import argparse
import logging
import sys

from signac import init_project
from signac import get_project

from . import __version__
from . import template


logger = logging.getLogger(__name__)


def main_init(args):
    "Initialize a FlowProject from one of the templates defined in the template module."
    if not args.alias.isidentifier():
        raise ValueError(
            "The alias '{}' is not a valid Python identifier and can therefore "
            "not be used as a FlowProject alias.".format(args.alias))
    try:
        get_project()
    except LookupError:
        init_project(name=args.alias)
        print("Initialized signac project with name '{}' in "
              "current directory.".format(args.alias), file=sys.stderr)
    try:
        return template.init(alias=args.alias, template=args.template)
    except OSError as e:
        raise RuntimeError(
            "Error occurred while trying to initialize a flow project: {}".format(e))


def main():
    """Main entry function for the 'flow' command line tool.

    This function defines the main 'flow' command line interface which can be used
    to initialize FlowProject modules from different templates as well as print the
    version of the installed signac-flow package.
    """
    parser = argparse.ArgumentParser(
        description="flow provides the basic components to set up workflows for "
                    "projects as part of the signac framework.")
    parser.add_argument(
        '--debug',
        action='store_true',
        help="Show traceback on error for debugging.")
    parser.add_argument(
        '--version',
        action='store_true',
        help="Display the version number and exit.")

    subparsers = parser.add_subparsers()

    # the flow init command
    parser_init = subparsers.add_parser('init', help="Initialize a signac-flow project.")
    parser_init.set_defaults(func=main_init)
    parser_init.add_argument(
        "alias",
        type=str,
        nargs='?',
        default='project',
        help="Name of the flow project module to initialize. "
             "This name will also be used to initialize a signac project in case that "
             "no project was initialized prior to calling 'init'.")
    parser_init.add_argument(
        '-t', '--template',
        type=str,
        choices=tuple(sorted(template.TEMPLATES)),
        default='minimal',
        help="Specify a specific to template to use.")

    if '--version' in sys.argv:
        print('signac-flow', __version__)
        sys.exit(0)

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    if not hasattr(args, 'func'):
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
        sys.stderr.write('{}\n'.format(str(error)))
        if args.debug:
            raise
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
