# Copyright (c) 2023 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""TO DO."""
import git

from .util import collect_metadata


def collect_metadata_with_git(operation, job):
    """TODO."""
    repo = git.Repo(job._project.path)
    metadata = collect_metadata(operation, job)
    metadata["project"]["git"] = {
        "commit_id": str(repo.commit()),
        "dirty": repo.is_dirty(),
    }
    return metadata
