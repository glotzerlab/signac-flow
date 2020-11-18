#!/usr/bin/env python
# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Extract generated templates into a signac project for simplified inspection."""

import argparse
import os

import generate_template_reference_data as gen
import signac

PROJECT_DIR = os.path.join(os.path.dirname(__file__), "./template_reference_data")


def main(args):
    if not os.path.exists(PROJECT_DIR):
        os.makedirs(PROJECT_DIR)
    elif args.force:
        import shutil

        shutil.rmtree(PROJECT_DIR)
        os.makedirs(PROJECT_DIR)
    else:
        return

    p = signac.init_project(name=gen.PROJECT_NAME, root=PROJECT_DIR)
    p.import_from(origin=gen.ARCHIVE_DIR)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate reference submission scripts for various environments"
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Recreate the unarchived data space even if the directory already exists.",
    )
    main(parser.parse_args())
