# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import io
import os
import sys

import generate_status_reference_data as gen
import signac
from test_project import redirect_stderr, redirect_stdout


def test_print_status():

    # Force asserts to show the full file when failures occur.
    # Useful to debug errors that arise.

    # Must import the data into the project.
    with signac.TemporaryProject(name=gen.PROJECT_NAME) as p, signac.TemporaryProject(
        name="StatusProject"
    ) as status_pr:
        gen.init(p)
        fp = gen._TestProject.get_project(root=p.root_directory())
        status_pr.import_from(origin=gen.ARCHIVE_DIR)
        reference = []
        generated = []
        for job in status_pr:
            kwargs = job.statepoint()
            tmp_out = io.TextIOWrapper(io.BytesIO(), sys.stdout.encoding)
            with open(os.devnull, "w") as devnull:
                with redirect_stderr(devnull):
                    with redirect_stdout(tmp_out):
                        fp.print_status(**kwargs)

                    tmp_out.seek(0)
                    msg = f"---------- Status options {kwargs} for job {job}."
                    generated.extend([msg] + tmp_out.read().splitlines())

                    with open(job.fn("status.txt")) as file:
                        reference.extend([msg] + file.read().splitlines())
        assert "\n".join(generated) == "\n".join(reference)
