# Copyright (c) 2021 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import subprocess
from tempfile import TemporaryDirectory

import pytest
import signac

import flow


class ExitCodeError(RuntimeError):
    pass


class TestCLI:
    @pytest.fixture(autouse=True)
    def setUp(self, request):
        pythonpath = os.environ.get("PYTHONPATH")
        if pythonpath is None:
            pythonpath = [os.getcwd()]
        else:
            pythonpath = [os.getcwd()] + pythonpath.split(":")
        os.environ["PYTHONPATH"] = ":".join(pythonpath)
        self.tmpdir = TemporaryDirectory(prefix="signac-flow_")
        request.addfinalizer(self.tmpdir.cleanup)
        self.cwd = os.getcwd()
        os.chdir(self.tmpdir.name)
        request.addfinalizer(self.return_to_cwd)

    def return_to_cwd(self):
        os.chdir(self.cwd)

    def call(self, command, input=None, shell=False, error=False, raise_error=True):
        p = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=shell,
        )
        if input:
            p.stdin.write(input.encode())
        out, err = p.communicate()
        if p.returncode != 0 and raise_error:
            raise ExitCodeError(f"STDOUT='{out}' STDERR='{err}'")
        return err.decode() if error else out.decode()

    def test_print_usage(self):
        with pytest.raises(ExitCodeError):
            self.call("python -m flow".split())

        out = self.call("python -m flow".split(), raise_error=False)
        assert "usage:" in out

    def test_version(self):
        out = self.call("python -m flow --version".split())
        assert f"signac-flow {flow.__version__}" in out

    def test_help(self):
        out = self.call("python -m flow --help".split())
        assert "positional arguments:" in out
        assert "optional arguments:" in out

    def test_init_flowproject(self):
        self.call("python -m flow init".split())
        assert str(signac.get_project()) == "project"
        assert os.path.exists("project.py")

    def test_init_flowproject_alias(self):
        self.call("python -m flow init my_project".split())
        assert str(signac.get_project()) == "my_project"
        assert os.path.exists("my_project.py")

    def test_init_flowproject_fail_if_exists(self):
        self.call("python -m flow init".split())
        assert str(signac.get_project()) == "project"
        assert os.path.exists("project.py")
        with pytest.raises(ExitCodeError):
            self.call("python -m flow init".split())

    @pytest.mark.parametrize("template", flow.template.TEMPLATES)
    def test_init_flowproject_template(self, template):
        self.call(f"python -m flow init -t {template}".split())
        assert str(signac.get_project()) == "project"
        assert os.path.exists("project.py")
