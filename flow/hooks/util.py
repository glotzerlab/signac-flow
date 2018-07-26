# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import time


def collect_metadata(operation):
    return {
        '_metadata_schema_version': 1,
        'time': time.time(),
        'project': {
            'path': operation.job._project.root_directory(),
            'git': None,
        },
        'job-operation': {
            'name': operation.name,
            'cmd': operation.cmd,
            'directives': operation.directives,
            'job_id': operation.job.get_id(),
        }
    }
