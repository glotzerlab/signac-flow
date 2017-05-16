import argparse
import inspect

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
    args = parser.parse_args()

    project = get_project()

    if len(args.jobid):
        jobs = (project.open_job(id=jid) for jid in args.jobid)
    else:
        jobs = project

    for job in jobs:
        try:
            module = inspect.getmodule(inspect.currentframe().f_back)
            operation = getattr(module, args.operation)
        except AttributeError:
            raise KeyError("Unknown operation '{}'.".format(args.operation))
        else:
            operation(job)
