from flow import FlowProject


class _HooksTestProject(FlowProject):
    pass


def set_true(key, operation_name, job):
    job.doc[f"{operation_name}_{key}"] = True


def set_true_with_error(key, operation_name, error, job):
    job.doc[f"{operation_name}_{key}"] = (True, error)


@_HooksTestProject.operation
@_HooksTestProject.hook.on_start(
    lambda operation_name, job: set_true("start", operation_name, job))
@_HooksTestProject.hook.on_finish(
    lambda operation_name, job: set_true("finish", operation_name, job))
@_HooksTestProject.hook.on_success(
    lambda operation_name, job: set_true("success", operation_name, job))
@_HooksTestProject.hook.on_fail(
    lambda operation_name, error, job: set_true_with_error(
        "fail", operation_name, error, job))
def base(job):
    if job.sp.raise_exception:
        raise RuntimeError


if __name__ == "__main__":
    _HooksTestProject().main()
