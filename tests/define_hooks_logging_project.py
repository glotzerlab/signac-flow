import logging

from define_hooks_test_project import HOOKS_ERROR_MESSAGE

import flow
from flow import FlowProject
from flow.hooks import LogOperations

HOOKS_RUNTIME_MESSAGE = "Runtime message."


class _HooksLogOperationsProject(FlowProject):
    pass


def log_operation(fn_log_file, trigger):
    lo = LogOperations(fn_log_file).log_operation
    return trigger(lambda operation, job: lo(operation, job))


def log_operation_on_exception(fn_log_file):
    lo = LogOperations(fn_log_file).log_operation_on_exception
    return _HooksLogOperationsProject.operation_hooks.on_exception(
        lambda operation, error, job: lo(operation, error, job)
    )


def set_logger(job):
    logger = logging.getLogger(str(job))
    sh = logging.StreamHandler()
    logger.addHandler(sh)
    return logger


on_start = _HooksLogOperationsProject.operation_hooks.on_start
on_exit = _HooksLogOperationsProject.operation_hooks.on_exit
on_success = _HooksLogOperationsProject.operation_hooks.on_success


@_HooksLogOperationsProject.operation
@log_operation("base_start.log", on_start)
@log_operation("base_success.log", on_success)
@log_operation_on_exception("base_exception.log")
def base(job):
    logger = set_logger(job)

    logger.info(HOOKS_RUNTIME_MESSAGE)

    if job.sp.raise_exception:
        raise RuntimeError(HOOKS_ERROR_MESSAGE)


@_HooksLogOperationsProject.operation
@log_operation("base_cmd_success.log", on_success)
@log_operation_on_exception("base_cmd_exception.log")
@flow.cmd
def base_cmd(job):
    if job.sp.raise_exception:
        return "exit 42"
    else:
        return "touch base_cmd.txt"


if __name__ == "__main__":
    _HooksLogOperationsProject().main()
