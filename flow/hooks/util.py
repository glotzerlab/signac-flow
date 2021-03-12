# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from datetime import datetime, timezone


def collect_metadata(operation):
    return {
        # the metadata schema version:
        "_schema_version": "1",
        "time": datetime.now(timezone.utc).isoformat(),
        "project": {
            "path": operation.job._project.root_directory(),
            # the project schema version:
            "schema_version": operation.job._project.config.get("schema_version"),
        },
        "job-operation": {
            "name": operation.name,
            "cmd": operation.cmd,
            "directives": operation.directives,
            "job_id": operation.job.get_id(),
        },
    }
