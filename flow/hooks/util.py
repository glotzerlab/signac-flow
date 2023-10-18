# Copyright (c) 2023 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define a function to collect metadata on the operation and job."""
from datetime import datetime, timezone


def collect_metadata(operation, job):
    """Collect metadata related to the operation and job.

    Returns a directory including schema version, time, project, and job-operation.

    We can no longer track the following because we take in the operation name as a
    xstring rather than as an object, but I think this is still super useful
    information.

    Should we just drop it or see if there's still some way to access this info?

    "cmd": operation.cmd,
    "directives": operation.directives,
    """
    return {
        # the metadata schema version:
        "_schema_version": "1",
        "time": datetime.now(timezone.utc).isoformat(),
        "project": {
            "path": job.project.path,
            # the project schema version:
            "schema_version": job.project.config.get("schema_version"),
        },
        "job-operation": {
            "name": operation,
            "job_id": job.id,
        },
    }
