# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import io
import os
import sys

import generate_status_reference_data as gen
import pytest
import signac


@pytest.fixture(params=[True, False])
def hide_progress_bar(request):
    return request.param


@pytest.fixture(params=["thread", "process", "none"])
def parallelization(request):
    return request.param


def test_hide_progress_bar(hide_progress_bar, parallelization):

    with signac.TemporaryProject(name=gen.PROJECT_NAME) as p, signac.TemporaryProject(
        name=gen.STATUS_OPTIONS_PROJECT_NAME
    ) as status_pr:
        gen.init(p)
        fp = gen._TestProject.get_project(root=p.root_directory())
        fp.config["status_parallelization"] = parallelization
        status_pr.import_from(origin=gen.ARCHIVE_PATH)
        for job in status_pr:
            kwargs = job.statepoint()
            tmp_err = io.TextIOWrapper(io.BytesIO(), sys.stderr.encoding)
            fp.print_status(**kwargs, err=tmp_err, hide_progress=hide_progress_bar)
            tmp_err.seek(0)
            generated_tqdm = tmp_err.read()
            if hide_progress_bar:
                assert "Fetching status" not in generated_tqdm
            else:
                assert "Fetching status" in generated_tqdm


def test_print_status():

    # Must import the data into the project.
    with signac.TemporaryProject(name=gen.PROJECT_NAME) as p, signac.TemporaryProject(
        name=gen.STATUS_OPTIONS_PROJECT_NAME
    ) as status_pr:
        gen.init(p)
        fp = gen._TestProject.get_project(root=p.path)
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
