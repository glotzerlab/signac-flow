from define_hooks_test_project import HOOKS_ERROR_MESSAGE

from flow import FlowProject
from flow.hooks import TrackOperations


class _HooksTrackOperations(FlowProject):
    pass


LOG_FILENAME = "operations.log"


track_operations = TrackOperations(strict_git=False)
track_operations_with_file = TrackOperations(LOG_FILENAME, strict_git=False)


@track_operations_with_file.install_operation_hooks(_HooksTrackOperations)
@track_operations.install_operation_hooks(_HooksTrackOperations)
@_HooksTrackOperations.operation
def base(job):
    if job.sp.raise_exception:
        raise RuntimeError(HOOKS_ERROR_MESSAGE)


@track_operations_with_file.install_operation_hooks(_HooksTrackOperations)
@track_operations.install_operation_hooks(_HooksTrackOperations)
@_HooksTrackOperations.operation(cmd=True, with_job=True)
def cmd(job):
    if job.sp.raise_exception:
        return "exit 42"
    else:
        return "touch base_cmd.txt"


track_operations_strict = TrackOperations()
track_operations_strict_with_file = TrackOperations(LOG_FILENAME)


@track_operations_strict.install_operation_hooks(_HooksTrackOperations)
@track_operations_strict_with_file.install_operation_hooks(_HooksTrackOperations)
@_HooksTrackOperations.operation
def strict_base(job):
    if job.sp.raise_exception:
        raise RuntimeError(HOOKS_ERROR_MESSAGE)


@track_operations_strict.install_operation_hooks(_HooksTrackOperations)
@track_operations_strict_with_file.install_operation_hooks(_HooksTrackOperations)
@_HooksTrackOperations.operation(cmd=True, with_job=True)
def strict_cmd(job):
    if job.sp.raise_exception:
        return "exit 42"
    else:
        return "touch base_cmd.txt"


if __name__ == "__main__":
    _HooksTrackOperations().main()
