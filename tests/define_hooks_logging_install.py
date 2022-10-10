from define_hooks_logging_project import (
    HOOKS_ERROR_MESSAGE,
    HOOKS_RUNTIME_MESSAGE,
    set_logger,
)

import flow
from flow.hooks import LogOperations


class _HooksLogOperationsProject(flow.FlowProject):
    pass


@_HooksLogOperationsProject.operation
def base(job):
    logger = set_logger(job)

    logger.info(HOOKS_RUNTIME_MESSAGE)

    if job.sp.raise_exception:
        raise RuntimeError(HOOKS_ERROR_MESSAGE)


@_HooksLogOperationsProject.operation
@flow.cmd
def base_cmd(job):
    if job.sp.raise_exception:
        return "exit 42"
    else:
        return "touch base_cmd.txt"


if __name__ == "__main__":
    LogOperations("operations.log").install_project_hooks(
        _HooksLogOperationsProject()
    ).main()
