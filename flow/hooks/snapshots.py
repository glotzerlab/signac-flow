# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import gzip
import tarfile
import logging
import tempfile
import shutil
import hashlib
import json
import re
from contextlib import contextmanager
from tempfile import NamedTemporaryFile

from signac import Collection

try:
    from .git_util import collect_metadata_with_git as collect_metadata
except ImportError:
    from .util import collect_metadata


DEFAULT_SNAPSHOTS_DIRECTORY = '.signac/snapshots'

logger = logging.getLogger('snapshot')


@contextmanager
def _archive_project(project, workspace, ignore):
    logger.info("Archiving project '{}'...".format(project))

    wd = os.path.relpath(project.workspace(), project.root_directory())

    def filter(info):
        if not workspace and info.isdir() and os.path.relpath(info.name, 'project').startswith(wd):
            return
        if ignore is not None and re.match(ignore, info.name):
            return
        return info

    with tempfile.TemporaryDirectory() as tmpdir:
        fn_snapshot = os.path.join(tmpdir, 'project.tar')
        with tarfile.open(fn_snapshot, 'w') as tarball:
            tarball.add(project.root_directory(), 'project', filter=filter)
        yield fn_snapshot, tarball


class Snapshot:
    """Track the project source code and/or workspace with snapshot archives.

    :param workspace:
        If True, include the workspace in the snapshot.
    :param compress:
        Compress the archive containing the snapshot.
    :param ignore:
        Ignore all files or directories that match the given pattern.
    :param directory:
        Specify the name of the directory where the snapshots will be stored.
    """

    def __init__(self, workspace=False, ignore=None, compress=False,
                 directory=DEFAULT_SNAPSHOTS_DIRECTORY):
        self.compress = compress
        self.workspace = workspace
        self.ignore = ignore

        self._directory = directory

    def archive_project(self, operation):
        project = operation.job._project
        metadata = collect_metadata(operation)
        with NamedTemporaryFile(mode='w') as metadatafile:
            json.dump(collect_metadata(operation), metadatafile)
            metadatafile.flush()
            os.makedirs(project.fn(self._directory), exist_ok=True)
            with _archive_project(
                    project=project, workspace=self.workspace,
                    ignore=self.ignore) as (fn_tmp, tarball):

                # Determine snapshot id
                with open(fn_tmp, 'rb') as archive_file:
                    m = hashlib.sha256()
                    m.update(archive_file.read())

                snapshot_id = m.hexdigest()
                logger.info("Project snapshot id: {}".format(snapshot_id))

                fn_snapshot = project.fn(os.path.join(self._directory, '{}.tar{}'.format(
                    snapshot_id, '.gz' if self.compress else '')))

                if os.path.isfile(fn_snapshot):
                    logger.debug("Snapshot file '{}' already exists.".format(fn_snapshot))
                else:
                    if self.compress:
                        with open(fn_tmp, 'rb') as archive_file:
                            with gzip.open(fn_snapshot, 'wb') as zipfile:
                                zipfile.write(archive_file.read())
                    else:
                        shutil.move(fn_tmp, fn_snapshot)
                    logger.info("Created snapshot file '{}'.".format(fn_snapshot))

        with Collection.open(os.path.join(self._directory, 'metadata.txt')) as c:
            c[snapshot_id] = metadata

    def install_hooks(self, project):
        project.hooks.on_start.append(self.archive_project)
        return project

    __call__ = install_hooks
