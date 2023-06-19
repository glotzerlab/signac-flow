# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Built in execution hook for basic tracking."""
import json

try:
    import git
except ImportError:
    from .util import collect_metadata

    GIT = False
else:
    from .git_util import collect_metadata_with_git as collect_metadata

    GIT = True


FN_LOGFILE = ".operations_log.txt"


class TrackOperations:
    """:class:`~.TrackOperations` tracks information about the execution of operations to a logfile.

    This hooks provides information, optionally, on the start, successful completion, and/or
    erroring of one or more operations in a `flow.FlowProject` instance. The logs are stored in a
    file given by the parameter ``fn_logfile``. This file will be appended to if it already exists.
    The default formating for the log provides the [time, job id, log level, and log message].
    .. note::
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


    The code block below provides an example of how install  :class:`~.TrackOperations` to a
    instance of :class:`~.FlowProject`

    .. code-block:: python
        from flow import FlowProject
        from flow.hooks import TrackOperations  # Import build


        class Project(FlowProject):
            pass


        # Do something


        if __name__ == "__main__":
            project = Project()
            project = TrackOperations().install_project_hooks(project)
            project.main()



    Parameters
    ----------
    fn_logfile: log filename
        The name of the log file in the job workspace. Default is "execution-record.log".
    """

    def __init__(self, strict_git=True):
        if strict_git and not GIT:
            raise RuntimeError(
                "Unable to collect git metadata from the repository, "
                "because the GitPython package is not installed.\n\n"
                "You can use '{}(strict_git=False)' to ignore this "
                "error.".format(type(self).__name__)
            )
        self.strict_git = strict_git

    def log_operation(self, stage):
        """Define log_operation to collect metadata of job workspace and write to logfiles."""

        def _log_operation(operation, job, error=None):
            if self.strict_git:
                if git.Repo(job._project.path).is_dirty():
                    raise RuntimeError(
                        "Unable to reliably log operation, because the git repository in "
                        "the project root directory is dirty.\n\nMake sure to commit all "
                        "changes or ignore this warning by setting '{}(strict_git=False)'.".format(
                            type(self).__name__
                        )
                    )
            metadata = collect_metadata(operation, job)

            # Add additional execution-related information to metadata.
            metadata["stage"] = stage
            metadata["error"] = None if error is None else str(error)

            # Write metadata to collection inside job workspace.
            with open(job.fn(FN_LOGFILE), "a") as logfile:
                # HELP: Is there any reason we used Collections instead of a regular dictionary?
                logfile.write(json.dumps(metadata) + "\n")

        return _log_operation

    def on_start(self, operation, job):
        """Track the start of execution of a given job(s) operation pair."""
        self.log_operation(stage="prior")(operation, job)

    def on_success(self, operation, job):
        """Track the successful completion of a given job(s) operation pair."""
        self.log_operation(stage="after")(operation, job)

    def on_exception(self, operation, error, job):
        """Log errors raised during the execution of a specific job(s) operation pair."""
        self.log_operation(stage="after")(operation, job, error)

    def install_operation_hooks(self, op, project_cls=None):
        """Decorate operation to install track operation to one operation in a Signac-flow project.

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
        """Install track operation to all operations in a Signac-flow project.

        Parameters
        ----------
        project : flow.FlowProject
            THe project to install project wide hooks.
        """
        project.project_hooks.on_start.append(self.on_start)
        project.project_hooks.on_success.append(self.on_success)
        project.project_hooks.on_exception.append(self.on_exception)
        return project

    __call__ = install_project_hooks
