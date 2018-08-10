# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import tarfile
import logging
import tempfile
import shutil
import json
import re
from datetime import datetime
from contextlib import contextmanager
from tempfile import NamedTemporaryFile

try:
    from .git_util import collect_metadata_with_git as collect_metadata
except ImportError:
    from .util import collect_metadata


DEFAULT_TIME_FORMAT = "%Y-%m-%dT%H_%M_%S.%f"
DEFAULT_SNAPSHOTS_DIRECTORY = '.snapshots'
DEFAULT_FILENAME_OPERATION_METADATA = 'operation.json'

logger = logging.getLogger('snapshot')


@contextmanager
def _archive_project(project, compress=False,
                     exclude_workspace=False, exclude_hidden=True, exclude=None):
    logger.info("Archiving '{}'...".format(project))

    wd = os.path.relpath(project.workspace(), project.root_directory())

    def filter(info):
        if exclude_hidden and os.path.basename(info.name).startswith('.'):
            return
        if exclude_workspace and info.isdir():
            if os.path.relpath(info.name, 'project').startswith(wd):
                return
        if exclude is not None:
            if re.match(exclude, info.name):
                return
        return info

    with tempfile.TemporaryDirectory() as tmpdir:
        fn_archive = os.path.join(tmpdir, 'project.tar')
        if compress:
            fn_archive += '.gz'
        with tarfile.open(fn_archive, 'w:gz' if compress else 'w') as tarball:
            tarball.add(project.root_directory(), 'project', filter=filter)
            yield fn_archive, tarball


class SnapshotProject(object):
    """Snapshots the project (excluding the workspace by default) before executing an operation.

    :param compress:
        Compress the archive containing the snapshot.
    :param exclude_workspace:
        Do not copy the workspace into the snapshot archive (default=True).
    :param exclude_hidden:
        Ignore all hidden files from the snapshot.
    :param exclude:
        Exclude all files that match the provided regular expression.
    :param directory:
        Specify the name of the directory where the snapshots will be stored.
    :param time_format:
        Specify the name of the snapshot archive file, which is based on the time
        when the snapshot was taken.
    """
    def __init__(self, compress=False,
                 exclude_workspace=True, exclude_hidden=True, exclude=None,
                 directory=DEFAULT_SNAPSHOTS_DIRECTORY, time_format=DEFAULT_TIME_FORMAT,
                 filename_operation_metadata=DEFAULT_FILENAME_OPERATION_METADATA):
        self.compress = compress
        self.exclude_workspace = exclude_workspace
        self.exclude_hidden = exclude_hidden
        self.exclude = exclude

        self._directory = directory
        self._time_format = time_format
        self._fn_operation_metadata = filename_operation_metadata

    def archive_project(self, operation):
        project = operation.job._project
        ts_f = datetime.utcnow().strftime(self._time_format)
        fn_archive = os.path.join(self._directory, '{}_{}.tar'.format(ts_f, operation.name))
        with NamedTemporaryFile(mode='w') as metadatafile:
            json.dump(collect_metadata(operation), metadatafile)
            metadatafile.flush()
            os.makedirs(operation.job.fn(self._directory), exist_ok=True)
            with _archive_project(project=project, compress=self.compress,
                                  exclude_workspace=self.exclude_workspace,
                                  exclude_hidden=self.exclude_hidden,
                                  exclude=self.exclude) as (fn_tmp, tarball):
                tarball.add(metadatafile.name, self._fn_operation_metadata)
                shutil.move(fn_tmp, operation.job.fn(fn_archive))

    def install_hooks(self, project):
        project.hooks.on_start.append(self.archive_project)
        return project
