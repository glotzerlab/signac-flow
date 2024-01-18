# Copyright (c) 2023 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Built in execution hook for basic tracking."""
import json

from .util import collect_metadata

try:
    from .git_util import collect_git_metadata
except ImportError:
    GIT = False
else:
    GIT = True


_DEFAULT_FILENAME = "signac_execution_history.log"


class TrackOperations:
    """:class:`~.TrackOperations` tracks information about the execution of operations to a logfile.

    This hook can provides information on the start, successful completion, and/or error of
    one or more operations in a :class:`~.FlowProject` instance. The logs are stored in the file
    given by ``log_filename`` within the job's path. The file will be appended to if it already
    exists.

    The hooks stores metadata regarding the execution of the operation and the state of the
    project at the time of execution, error, and/or completion. The data will also include
    information about the git status if the project is detected as a git repo and
    ``GitPython`` is installed in the environment.

    Each call to the hook adds a single JSON line to the log file. These can be
    read using the `json` builtin package or :meth:`~.TrackOperations.read_log`.

    The current schema has the following structure:

    - ``time``: The time of querying the metadata.
    - ``stage``: Whether the hook was executed either "prior" or "after" the associated
      operation's execution.
    - ``error``: The error message on executing the operation if any.
    - ``project``
       - ``path``: Filepath to the project
       - ``schema_version``: The project's schema version
    - ``operation``: The operation name
    - ``job_id``: The job id
    - ``git``:
       - ``commit_id``: The current commit of the project's git repo.
       - ``dirty``: Whether the project's repo has uncommitted changes or not.
    - ``_schema_version``: The metadata storage's schema version.


    Warning
    -------
    This class will raise an exception when ``strict_git`` is set to ``True`` and either
    GitPython is not available or the repository contains uncommitted changes (i.e. is "dirty").

    Examples
    --------
    The following example will install :class:`~.TrackOperations` at the operation level.

    .. code-block:: python

        from flow import FlowProject
        from flow.hooks import TrackOperations


        class Project(FlowProject):
            pass


        track = TrackOperation()


        @track.install_operation_hooks(Project)
        @Project.operation
        def foo(job):
            pass


    The code block below provides an example of how install :class:`~.TrackOperations` to a
    instance of :class:`~.FlowProject`

    .. code-block:: python

        from flow import FlowProject
        from flow.hooks import TrackOperations


        class Project(FlowProject):
            pass


        if __name__ == "__main__":
            project = Project()
            project = TrackOperations().install_project_hooks(project)
            project.main()


    Parameters
    ----------
    log_filename : str, optional
        The name of the log file in the job workspace. Defaults to "signac_execution_history.log".
    strict_git : bool, optional
        Whether to fail if ``GitPython`` cannot be imported or if there are uncommitted changes
        to the project's git repo. Defaults to ``True``.
    """

    def __init__(self, log_filename=_DEFAULT_FILENAME, strict_git=True):
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
        with open(job.fn(self.log_filename), "a") as logfile:
            logfile.write(json.dumps(metadata) + "\n")

    def _get_metadata(self, operation, job, stage, error=None):
        """Define log_operation to collect metadata of job workspace and write to logfiles."""
        # Add execution-related information to metadata.
        metadata = {"stage": stage, "error": None if error is None else str(error)}
        metadata.update(collect_metadata(operation, job))
        if GIT:
            git_metadata = collect_git_metadata(job)
            if self.strict_git and git_metadata["dirty"]:
                raise RuntimeError(
                    "Unable to reliably log operation, because the git repository in "
                    "the project root directory is dirty.\n\nMake sure to commit all "
                    "changes or ignore this warning by setting '{}(strict_git=False)'.".format(
                        type(self).__name__
                    )
                )
            metadata["project"]["git"] = git_metadata
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
            An operation function to log or a subclass of :class:`~.FlowProject` if
            ``project_cls`` is ``None``.
        project_cls : type
            A subclass of :class:`~.FlowProject`.
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

    @classmethod
    def read_log(cls, job, log_filename=_DEFAULT_FILENAME):
        """Return the execution log data as a list of dictionaries.

        Parameters
        ----------
        job : signac.job.Job
            The job to read the execution history of.
        log_filename : str, optional
            The name of the log file in the job workspace. Defaults to
            "signac_execution_history.log".

        Returns
        -------
        log : list[dict[str, any]]
            Returns the job's current execution history for logged operations.
        """
        with open(job.fn(log_filename)) as fh:
            return [json.loads(line) for line in fh.readlines() if line != ""]
