# Copyright (c) 2023 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define a function to collect metadata with git."""
import git


def collect_git_metadata(job):
    """Collect git metadata for a given workspace.

    The information includes the commit ID and a flag indicating if the
    repository is dirty (has uncommitted changes).
    """
    repo = git.Repo(job.project.path)
    return {"commit_id": str(repo.commit()), "dirty": repo.is_dirty()}
