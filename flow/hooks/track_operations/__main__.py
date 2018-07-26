# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import argparse

from signac import get_project
from signac import Collection

from . import FN_LOGFILE


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'jobid',
        help="Specify for which job the log is to be shown.")
    parser.add_argument(
        '--fn-logfile',
        default=FN_LOGFILE,
        help="The name of the log collection file within each job workspace.")
    args = parser.parse_args()

    project = get_project()
    job = project.open_job(id=args.jobid)

    with Collection.open(job.fn(args.fn_logfile)) as logfile:
        for doc in logfile:
            print(doc)


if __name__ == '__main__':
    main()
