# Copyright (c) 2023 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Built in execution hook for basic tracking."""
import json

from .util import collect_metadata

try:
    import git
except ImportError:
    GIT = False
else:
    from .git_util import collect_git_metadata

    GIT = True


class TrackOperations:
    """:class:`~.TrackOperations` tracks information about the execution of operations to a logfile.

    This hooks can provides information on the start, successful completion, and/or
    erroring of one or more operations in a `flow.FlowProject` instance. The logs are stored in a
    file given by the parameter ``fn_logfile``. This file will be appended to if it already exists.
    The default formating for the log provides the [time, job id, log level, and log message].

    Note
    ----
    All tracking is performed at the INFO level. To ensure outputs are captured in log files,
    use the `--debug` flag when running or submitting jobs, or specify
    `submit_options=--debug` in your directives (example shown below).

    Examples
    --------
    The following example will install :class:`~.TrackOperations` at the operation level.
    Where the log will be stored in a file name `foo.log` in the job workspace.

    .. code-block:: python
        from flow import FlowProject
        from flow.hooks import TrackOperations


        class Project(FlowProject):
            pass


        def install_operation_track_hook(operation_name, project_cls):
            track = TrackOperation(f"{operation_name}.log")
            return lambda op: track.install_operation_hooks(op, project_cls)


        @install_operation_log_hook("foo", Project)
        @Project.operation(directives={
            "submit_options": "--debug"  # Always submit operation foo with the --debug flag
        })
        def foo(job):
            pass


    The code block below provides an example of how install :class:`~.TrackOperations` to a
    instance of :class:`~.FlowProject`

    .. code-block:: python
        from flow import FlowProject
        from flow.hooks import TrackOperations  # Import build


        class Project(FlowProject):
            pass


        if __name__ == "__main__":
            project = Project()
            project = TrackOperations().install_project_hooks(project)
            project.main()


    Parameters
    ----------
    log_filename: str, optional
        The name of the log file in the job workspace. If ``None`` store in a list in the job
        document in a key labeled ``f"{operation_name}"`` under the ``"execution_history"`` key.
        Defaults to ``None``.
    strict_git: bool, optional
        Whether to fail if ``GitPython`` cannot be imported. Defaults to ``True``.
    """

    def __init__(self, log_filename=None, strict_git=True):
        self.log_filename = log_filename
        if strict_git and not GIT:
            raise RuntimeError(
                "Unable to collect git metadata from the repository, "
                "because the GitPython package is not installed.\n\n"
                "You can use '{}(strict_git=False)' to ignore this "
                "error.".format(type(self).__name__)
            )
        self.strict_git = strict_git

    def _write_metadata(self, job, metadata):
        print(f"Writing... {metadata}.", flush=True)
        if self.log_filename is None:
            history = job.doc.setdefault("execution_history", {})
            # No need to store job id or operation name.
            operation_name = metadata.pop("job-operation")["name"]
            history.setdefault(operation_name, []).append(metadata)
            return
        with open(job.fn(self.log_filename), "a") as logfile:
            logfile.write(json.dumps(metadata) + "\n")

    def _get_metadata(self, operation, job, stage, error=None):
        """Define log_operation to collect metadata of job workspace and write to logfiles."""
        # Add execution-related information to metadata.
        metadata = {"stage": stage, "error": None if error is None else str(error)}
        metadata.update(collect_metadata(operation, job))
        if GIT:
            if self.strict_git and git.Repo(job.project.path).is_dirty():
                raise RuntimeError(
                    "Unable to reliably log operation, because the git repository in "
                    "the project root directory is dirty.\n\nMake sure to commit all "
                    "changes or ignore this warning by setting '{}(strict_git=False)'.".format(
                        type(self).__name__
                    )
                )
            metadata["project"]["git"] = collect_git_metadata(job)
        return metadata

    def on_start(self, operation, job):
        """Track the start of execution of an operation on a job."""
        self._write_metadata(job, self._get_metadata(operation, job, stage="prior"))

    def on_success(self, operation, job):
        """Track the successful completion of an operation on a job."""
        self._write_metadata(job, self._get_metadata(operation, job, stage="after"))

    def on_exception(self, operation, error, job):
        """Log errors raised during the execution of an operation on a job."""
        self._write_metadata(
            job, self._get_metadata(operation, job, stage="after", error=error)
        )

    def install_operation_hooks(self, op, project_cls=None):
        """Decorate operation to track execution.

        Parameters
        ----------
        op : function or type
            An operation function to log or a subclass of `flow.FlowProject` if ``project_cls`` is
            ``None``.
        project_cls : type
            A subclass of `flow.FlowProject`.
        """
        if project_cls is None:
            return lambda func: self.install_operation_hooks(func, op)
        project_cls.operation_hooks.on_start(self.on_start)(op)
        project_cls.operation_hooks.on_success(self.on_success)(op)
        project_cls.operation_hooks.on_exception(self.on_exception)(op)
        return op

    def install_project_hooks(self, project):
        """Install hooks to track all operations in a `flow.FlowProject`.

        Parameters
        ----------
        project : flow.FlowProject
            The project to install hooks on.
        """
        project.project_hooks.on_start.append(self.on_start)
        project.project_hooks.on_success.append(self.on_success)
        project.project_hooks.on_exception.append(self.on_exception)
        return project
