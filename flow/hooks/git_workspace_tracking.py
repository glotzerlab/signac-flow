# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import os
import json
import logging
from collections import defaultdict

import git

from .git_util import collect_metadata_with_git as collect_metadata


logger = logging.getLogger('git_tracking')

METADATA_DELIMITER = '-' * 10

GIT_COMMIT_MSG = """{title}

{metadata_delimiter}
{metadata}
{metadata_delimiter}

This commit was auto-generated.
"""


_WARNINGS = defaultdict(set)


def _ignore_hidden_files(root):
    "Do not commit hidden files to the git repository."
    with open(os.path.join(root, '.gitignore'), mode='a+') as file:
        file.seek(0)
        if '.*\n' not in file.readlines():
            file.write('.*\n')


def _get_or_init_git_repo(root):
    "Return the git repository for root and initialize it if necessary."
    try:
        return git.Repo(root)
    except git.exc.InvalidGitRepositoryError:
        logger.info("Initializing git repo for '{}'".format(root))
        _ignore_hidden_files(root)
        return git.Repo.init(root)


def _commit(repo, title, metadata):
    try:
        repo.git.commit('-m', GIT_COMMIT_MSG.format(
            title=title, metadata=json.dumps(metadata, indent=2),
            metadata_delimiter=METADATA_DELIMITER))
    except git.exc.GitCommandError as error:
        if "nothing to commit, working tree clean" in str(error):
            pass
        else:
            raise


def _get_commit_metadata(commit_msg):
    lines = commit_msg.splitlines()
    start = lines.index(METADATA_DELIMITER) + 1
    stop = lines.index(METADATA_DELIMITER, start)
    return json.loads('\n'.join(lines[start:stop]))


def get_project_source_git_metadata(project, strict=False):
    repo = git.Repo(project.root_directory())
    wd_rel = os.path.relpath(project.workspace(), project.root_directory())
    for path in repo.untracked_files:
        if os.path.commonprefix((wd_rel, path)):
            if project.workspace() not in _WARNINGS['workspace_not_ignored']:
                logger.warn("Skipping workspace directory for project '{}'.".format(project))
                _WARNINGS['workspace_not_ignored'].add(project.workspace())
            break
    if repo.is_dirty():
        if strict:
            raise RuntimeError(
                "Unable to get project source git commit id, repository is dirty.")
        else:
            if project.root_directory() not in _WARNINGS['git_dirty']:
                logger.warn("Unable to get project source git commit id, repository is dirty.")
                _WARNINGS['git_dirty'].add(project.root_directory())
    return {
        'commit_id': str(repo.commit()),
        'dirty': repo.is_dirty(),
    }


class TrackWorkspaceWithGit(object):

    def __init__(self, per_job=True, metadata_delimiter=METADATA_DELIMITER):
        self._per_job = per_job
        self._warnings = defaultdict(set)

    def install_hooks(self, project):
        project.hooks.on_start.append(self.commit_before)
        project.hooks.on_success.append(self.commit_after)
        project.hooks.on_fail.append(self.commit_after)
        return project

    def _get_repo(self, operation):
        if self._per_job:
            return _get_or_init_git_repo(root=operation.job.workspace())
        else:
            return _get_or_init_git_repo(root=operation.job._project.workspace())

    def commit_before(self, operation):
        metadata = collect_metadata(operation)
        metadata['stage'] = 'prior'
        repo = self._get_repo(operation)
        repo.git.add(A=True)
        _commit(repo, "Before executing operation {}.".format(operation), metadata)

    def commit_after(self, operation, error=None):
        metadata = collect_metadata(operation)
        metadata['stage'] = 'post'
        metadata['error'] = None if error is None else str(error)
        repo = self._get_repo(operation)
        repo.git.add(A=True)
        if error:
            _commit(repo, "Executed operation {}.\n\nThe execution failed "
                          "with error '{}'.".format(operation, error), metadata)
        else:
            _commit(repo, "Executed operation {}.".format(operation), metadata)
