from flow import FlowProject

HOOKS_ERROR_MESSAGE = "You raised an error! Hooray!"
HOOK_KEYS = ("start", "exit", "success", "exception")


class _HooksTestProject(FlowProject):
    pass


def set_job_doc(key):
    def set_true(operation_name, job):
        job.doc[f"{operation_name}_{key}"] = True

    return set_true


def set_job_doc_with_error(key=HOOK_KEYS[-1]):
    def set_true_with_error(operation_name, error, job):
        job.doc[f"{operation_name}_{key}"] = (True, error.args[0])

    return set_true_with_error


def raise_error(operation_name, job):
    raise RuntimeError(HOOKS_ERROR_MESSAGE)


@_HooksTestProject.operation_hooks.on_start(set_job_doc(HOOK_KEYS[0]))
@_HooksTestProject.operation_hooks.on_exit(set_job_doc(HOOK_KEYS[1]))
@_HooksTestProject.operation_hooks.on_success(set_job_doc(HOOK_KEYS[2]))
@_HooksTestProject.operation_hooks.on_exception(set_job_doc_with_error())
@_HooksTestProject.operation
def base(job):
    if job.sp.raise_exception:
        raise RuntimeError(HOOKS_ERROR_MESSAGE)


@_HooksTestProject.operation_hooks.on_start(set_job_doc(HOOK_KEYS[0]))
@_HooksTestProject.operation_hooks.on_exit(set_job_doc(HOOK_KEYS[1]))
@_HooksTestProject.operation_hooks.on_success(set_job_doc(HOOK_KEYS[2]))
@_HooksTestProject.operation_hooks.on_exception(set_job_doc_with_error())
@_HooksTestProject.operation(cmd=True, with_job=True)
def base_cmd(job):
    if job.sp.raise_exception:
        return "exit 42"
    else:
        return "touch base_cmd.txt"


@_HooksTestProject.operation
def base_no_decorators(job):
    if job.sp.raise_exception:
        raise RuntimeError(HOOKS_ERROR_MESSAGE)


@_HooksTestProject.operation(cmd=True, with_job=True)
def base_cmd_no_decorators(job):
    if job.sp.raise_exception:
        return "exit 42"
    else:
        return "touch base_cmd.txt"


@_HooksTestProject.operation_hooks.on_start(raise_error)
@_HooksTestProject.operation
def raise_exception_in_hook(job):
    pass


@_HooksTestProject.operation_hooks.on_start(raise_error)
@_HooksTestProject.operation(cmd=True, with_job=True)
def raise_exception_in_hook_cmd(job):
    pass


if __name__ == "__main__":
    _HooksTestProject().main()
