# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import io
import os
import sys

import generate_status_reference_data as gen
import signac


def test_print_status():
    # Must import the data into the project.
    with signac.TemporaryProject() as p, signac.TemporaryProject() as status_pr:
        gen.init(p)
        fp = gen._TestProject.get_project(path=p.path)
        status_pr.import_from(origin=gen.ARCHIVE_PATH)
        for job in status_pr:
            kwargs = job.statepoint()
            msg = f"---------- Status options {kwargs} for job {job}."
            with open(os.devnull, "w") as devnull:
                tmp_out = io.TextIOWrapper(io.BytesIO(), sys.stdout.encoding)
                fp.print_status(**kwargs, file=tmp_out, err=devnull)
                tmp_out.seek(0)
                generated = [msg] + tmp_out.read().splitlines()
            with open(job.fn("status.txt")) as file:
                reference = [msg] + file.read().splitlines()
            assert "\n".join(generated) == "\n".join(reference)
