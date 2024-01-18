# Copyright (c) 2023 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define a function to collect metadata on the operation and job."""
from datetime import datetime, timezone


def collect_metadata(operation, job):
    """Collect metadata related to the operation and job.

    Returns a directory including schema version, time, project, and job-operation.

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
        "operation": operation,
        "job_id": job.id,
    }
