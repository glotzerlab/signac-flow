from __future__ import print_function
import sys
import argparse
import logging
import inspect
from contextlib import contextmanager
from multiprocessing import Pool

from signac import get_project
from signac.common import six


def _get_operations():
    module = inspect.getmodule(inspect.currentframe().f_back.f_back)
    for name, obj in inspect.getmembers(module):
        if inspect.isfunction(obj):
            if six.PY2:
                signature = inspect.getargspec(obj)
            else:
                signature = inspect.getfullargspec(obj)
            if len(signature.args) == 1:
                yield name


def run(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser()

    parser.add_argument(
        'operation',
        type=str,
        choices=list(_get_operations()),
        help="The operation to execute.")
    parser.add_argument(
        'jobid',
        type=str,
        nargs='*',
        help="The job ids, as registered in the signac project. "
             "Omit to default to all statepoints.")
    parser.add_argument(
        '--np',
        type=int,
        default=0,
        help="Specify the number of cores to parallelize to. The "
             "default value of 0 means as many cores as are available.")
    parser.add_argument(
        '-t', '--timeout',
        type=int,
        help="A timeout in seconds after which the parallel execution "
             "of operations is canceled.")
    args = parser.parse_args()

    project = get_project()

    def _open_job_by_id(_id):
        try:
            return project.open_job(id=_id)
        except KeyError:
            msg = "Did not find job corresponding to id '{}'.".format(_id)
            raise KeyError(msg)
        except LookupError as error:
            raise LookupError("Multiple matches for id '{}'.".format(_id))

    if len(args.jobid):
        try:
            jobs = [_open_job_by_id(jid) for jid in args.jobid]
        except (KeyError, LookupError) as e:
            print(e, file=sys.stderr)
            sys.exit(1)
    else:
        jobs = project

    module = inspect.getmodule(inspect.currentframe().f_back)
    try:
        operation = getattr(module, args.operation)
    except AttributeError:
        raise KeyError("Unknown operation '{}'.".format(args.operation))

    # Serial execution
    if args.np == 1:
        for job in jobs:
            operation(job)

    # Parallel execution
    elif six.PY2:
        pool = Pool(None if args.np == 0 else args.np)

        # Due to Python 2.7 issue #8296 (http://bugs.python.org/issue8296) we
        # always need to provide a timeout to avoid issues with "hanging"
        # processing pools.
        timeout = sys.maxint if args.timeout is None else args.timeout
        pool.map_async(operation, jobs).get(timeout)
    else:
        with Pool(None if args.np == 0 else args.np) as pool:
            pool.map_async(operation, jobs).get(args.timeout)


@contextmanager
def redirect_log(job, filename='run.log', formatter=None, logger=None):
    if formatter is None:
        formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    if logger is None:
        logger = logging.getLogger()

    filehandler = logging.FileHandler(filename=job.fn('run.log'))
    filehandler.setFormatter(formatter)
    logger.addHandler(filehandler)
    try:
        yield
    finally:
        logger.removeHandler(filehandler)
