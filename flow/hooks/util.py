# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""TO DO."""
from datetime import datetime, timezone


def collect_metadata(operation, job):
    """TO DO."""
    return {
        # the metadata schema version:
        "_schema_version": "1",
        "time": datetime.now(timezone.utc).isoformat(),
        "project": {
            "path": job._project.path,
            # the project schema version:
            "schema_version": job._project.config.get("schema_version"),
        },
        "job-operation": {
            "name": operation,
            "job_id": job.id,
        },
    }
