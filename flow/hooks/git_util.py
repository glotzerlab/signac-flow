# Copyright (c) 2023 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define a function to collect metadata with git."""
import git

from .util import collect_metadata


def collect_metadata_with_git(operation, job):
    """Collect metadata for a given operation on a job, including git-related information.

    The git-related information includes the commit ID and a flag indicating if the
    repository is dirty (has uncommitted changes).
    """
    repo = git.Repo(job.project.path)
    metadata = collect_metadata(operation, job)
    metadata["project"]["git"] = {
        "commit_id": str(repo.commit()),
        "dirty": repo.is_dirty(),
    }
    return metadata
