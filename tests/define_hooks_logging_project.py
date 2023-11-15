import logging

from define_hooks_test_project import HOOKS_ERROR_MESSAGE

from flow import FlowProject
from flow.hooks import LogOperations

HOOKS_RUNTIME_MESSAGE = "Runtime message."


class _HooksLogOperationsProject(FlowProject):
    pass


operation_log = LogOperations("operations.log")


def set_logger(job):
    logger = logging.getLogger(str(job))
    sh = logging.StreamHandler()
    logger.addHandler(sh)
    return logger


@operation_log.install_operation_hooks(_HooksLogOperationsProject)
@_HooksLogOperationsProject.operation
def base(job):
    logger = set_logger(job)

    logger.info(HOOKS_RUNTIME_MESSAGE)

    if job.sp.raise_exception:
        raise RuntimeError(HOOKS_ERROR_MESSAGE)


@_HooksLogOperationsProject.operation_hooks.on_start(operation_log.on_start)
@_HooksLogOperationsProject.operation_hooks.on_success(operation_log.on_success)
@_HooksLogOperationsProject.operation_hooks.on_exception(operation_log.on_exception)
@_HooksLogOperationsProject.operation(cmd=True)
def base_cmd(job):
    if job.sp.raise_exception:
        return "exit 42"
    else:
        return "touch base_cmd.txt"


if __name__ == "__main__":
    _HooksLogOperationsProject().main()
