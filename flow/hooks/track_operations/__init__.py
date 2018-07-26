# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from signac import Collection

try:
    import git
except ImportError:
    from ..util import collect_metadata
    GIT = False
else:
    from ..git_util import collect_metadata_with_git as collect_metadata
    GIT = True


FN_LOGFILE = 'operations_log.txt'


class TrackOperations(object):

    def __init__(self, fn_logfile=FN_LOGFILE, strict_git=True):
        if strict_git and not GIT:
            raise RuntimeError("Unable to collect git metadata from the repository, "
                               "because the GitPython package is not installed.\n\n"
                               "You can use '{}(strict_git=False)' to ignore this "
                               "error.".format(type(self).__name__))
        self._fn_logfile = fn_logfile
        self.strict_git = strict_git

    def log_operation(self, stage):

        def _log_operation(operation, error=None):
            if self.strict_git:
                if git.Repo(operation.job._project.root_directory()).is_dirty():
                    raise RuntimeError(
                        "Unable to reliably log operation, because the git repository in "
                        "the project root directory is dirty.\n\nMake sure to commit all "
                        "changes or ignore this warning by setting '{}(strict_git=False)'.".format(
                            type(self).__name__))
            metadata = collect_metadata(operation)

            # Add additional execution-related information to metadata.
            metadata['stage'] = stage
            metadata['error'] = None if error is None else str(error)

            # Write metadata to collection inside job workspace.
            with Collection.open(operation.job.fn(self._fn_logfile)) as logfile:
                logfile.insert_one(metadata)

        return _log_operation

    def install_hooks(self, project):
        project.hooks.on_start.append(self.log_operation(stage='prior'))
        return project
