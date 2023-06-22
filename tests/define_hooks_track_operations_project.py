from define_hooks_test_project import HOOKS_ERROR_MESSAGE

from flow import FlowProject
from flow.hooks import TrackOperations


class _HooksTrackOperations(FlowProject):
    pass


track_operations = TrackOperations(strict_git=False)


"""
Strict Git False
"""


@_HooksTrackOperations.operation_hooks.on_start(
    TrackOperations(strict_git=False).on_start
)
@_HooksTrackOperations.operation_hooks.on_exception(
    TrackOperations(strict_git=False).on_exception
)
@_HooksTrackOperations.operation_hooks.on_success(
    TrackOperations(strict_git=False).on_success
)
@_HooksTrackOperations.operation
def strict_git_false(job):
    if job.sp.raise_exception:
        raise RuntimeError(HOOKS_ERROR_MESSAGE)


@_HooksTrackOperations.operation_hooks.on_start(
    TrackOperations(strict_git=False).on_start
)
@_HooksTrackOperations.operation_hooks.on_exception(
    TrackOperations(strict_git=False).on_exception
)
@_HooksTrackOperations.operation_hooks.on_success(
    TrackOperations(strict_git=False).on_success
)
@_HooksTrackOperations.operation(cmd=True, with_job=True)
def strict_git_false_cmd(job):
    if job.sp.raise_exception:
        return "exit 42"
    else:
        return "touch base_cmd.txt"


@track_operations.install_operation_hooks(_HooksTrackOperations)
@_HooksTrackOperations.operation
def strict_git_false_install(job):
    if job.sp.raise_exception:
        raise RuntimeError(HOOKS_ERROR_MESSAGE)


@track_operations.install_operation_hooks(_HooksTrackOperations)
@_HooksTrackOperations.operation(cmd=True, with_job=True)
def strict_git_false_install_cmd(job):
    if job.sp.raise_exception:
        return "exit 42"
    else:
        return "touch base_cmd.txt"


"""
Strict Git True
"""


try:

    @_HooksTrackOperations.operation_hooks.on_start(
        TrackOperations(strict_git=True).on_start
    )
    @_HooksTrackOperations.operation_hooks.on_exception(
        TrackOperations(strict_git=True).on_exception
    )
    @_HooksTrackOperations.operation_hooks.on_success(
        TrackOperations(strict_git=True).on_success
    )
    @_HooksTrackOperations.operation
    def strict_git_true(job):
        if job.sp.raise_exception:
            raise RuntimeError(HOOKS_ERROR_MESSAGE)

    @_HooksTrackOperations.operation_hooks.on_start(
        TrackOperations(strict_git=True).on_start
    )
    @_HooksTrackOperations.operation_hooks.on_exception(
        TrackOperations(strict_git=True).on_exception
    )
    @_HooksTrackOperations.operation_hooks.on_success(
        TrackOperations(strict_git=True).on_success
    )
    @_HooksTrackOperations.operation(cmd=True, with_job=True)
    def strict_git_true_cmd(job):
        if job.sp.raise_exception:
            return "exit 42"
        else:
            return "touch base_cmd.txt"

except RuntimeError:
    pass


if __name__ == "__main__":
    TrackOperations(strict_git=False).install_project_hooks(
        _HooksTrackOperations()
    ).main()
