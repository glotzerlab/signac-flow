# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""TO DO."""
from datetime import datetime, timezone


def collect_metadata(operation, job):
    """TODO.

    We can no longer track the following
    because we take in the operation name as a string
    rather than as an object, but I think this is
    still super useful information.

    Should we just drop it or see if there's still some
    way to access this info?

    "cmd": operation.cmd,
    "directives": operation.directives,
    """
    return {
        # the metadata schema version:
        "_schema_version": "1",
        "time": datetime.now(timezone.utc).isoformat(),
        "project": {
            "path": job._project.root_directory(),
            # the project schema version:
            "schema_version": job._project.config.get("schema_version"),
        },
        "job-operation": {
            "name": operation,
            "job_id": job.id,
        },
    }
