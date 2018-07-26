# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import git

from .util import collect_metadata


def collect_metadata_with_git(operation):
    repo = git.Repo(operation.job._project.root_directory())
    metadata = collect_metadata(operation)
    metadata['project']['git'] = {
        'commit_id': str(repo.commit()),
        'dirty': repo.is_dirty()
    }
    return metadata
