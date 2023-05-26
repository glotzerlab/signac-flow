import collections.abc
import inspect
import os
import subprocess
import sys
import tempfile
import uuid

import pytest
import signac

import flow
from flow.scheduling.base import ClusterJob, JobStatus, Scheduler
from flow.util.misc import _add_path_to_environment_pythonpath, _switch_to_directory


class MockScheduler(Scheduler):
    _jobs = {}  # needs to be singleton
    _scripts = {}
    _invalid_chars = []

    @classmethod
    def jobs(cls):
        for job in cls._jobs.values():
            for char in cls._invalid_chars:
                if char in job._id():
                    raise RuntimeError(f"Invalid character in job id: {char}")
            yield job

    @classmethod
    def submit(cls, script, _id=None, *args, **kwargs):
        if _id is None:
            for line in script:
                _id = str(line).strip()
                break
        cluster_id = uuid.uuid4()
        cls._jobs[cluster_id] = ClusterJob(_id, status=JobStatus.submitted)
        signac_path = os.path.dirname(os.path.dirname(os.path.abspath(signac.__file__)))
        flow_path = os.path.dirname(os.path.dirname(os.path.abspath(flow.__file__)))
        pythonpath = ":".join(
            [os.environ.get("PYTHONPATH", "")] + [signac_path, flow_path]
        )
        cls._scripts[cluster_id] = f"export PYTHONPATH={pythonpath}\n" + script
        return JobStatus.submitted

    @classmethod
    def step(cls):
        """Mock pushing of jobs through the queue."""
        remove = set()
        for cluster_id, job in cls._jobs.items():
            if job._status == JobStatus.inactive:
                remove.add(cluster_id)
            else:
                if job._status in (JobStatus.submitted, JobStatus.held):
                    job._status = JobStatus(job._status + 1)
                elif job._status == JobStatus.queued:
                    job._status = JobStatus.active
                    try:
                        with tempfile.NamedTemporaryFile() as tmpfile:
                            tmpfile.write(cls._scripts[cluster_id].encode("utf-8"))
                            tmpfile.flush()
                            subprocess.check_call(
                                ["/bin/bash", tmpfile.name], stderr=subprocess.DEVNULL
                            )
                    except Exception:
                        job._status = JobStatus.error
                        raise
                    else:
                        job._status = JobStatus.inactive
                else:
                    raise RuntimeError(f"Unable to process status '{job._status}'.")
        for cluster_id in remove:
            del cls._jobs[cluster_id]

    @classmethod
    def is_present(cls):
        return True

    @classmethod
    def reset(cls):
        cls._jobs.clear()


class TestProjectBase:
    project_class = signac.Project
    entrypoint = dict(path="")

    @pytest.fixture(autouse=True)
    def setUp(self, request):
        MockScheduler.reset()
        self._tmp_dir = tempfile.TemporaryDirectory(prefix="signac-flow_")
        request.addfinalizer(self._tmp_dir.cleanup)
        self.project = self.project_class.init_project(path=self._tmp_dir.name)
        self.cwd = os.getcwd()

    def switch_to_cwd(self):
        os.chdir(self.cwd)

    def mock_project(
        self, project_class=None, heterogeneous=False, config_overrides=None
    ):
        project_class = project_class if project_class else self.project_class
        project = project_class.get_project(path=self._tmp_dir.name)
        if config_overrides is not None:

            def recursive_update(d, u):
                for k, v in u.items():
                    if isinstance(v, collections.abc.Mapping):
                        d[k] = recursive_update(d.get(k, {}), v)
                    else:
                        d[k] = v
                return d

            config = signac._config._read_config_file(
                signac._config._get_project_config_fn(self._tmp_dir.name)
            )
            config = recursive_update(config, config_overrides)
            config.write()
            project = project_class.get_project(path=self._tmp_dir.name)
        for a in range(2):
            if heterogeneous:
                # Add jobs with only the `a` key without `b`.
                project.open_job(dict(a=a)).init()
                project.open_job(dict(a=dict(a=a))).init()
            # Tests assume that there are even and odd values of b
            for b in range(2):
                project.open_job(dict(a=a, b=b)).init()
                project.open_job(dict(a=dict(a=a), b=b)).init()
        project._entrypoint = self.entrypoint
        return project

    def call_subcmd(self, subcmd, stderr=subprocess.DEVNULL):
        # Determine path to project module and construct command.
        fn_script = inspect.getsourcefile(type(self.project))
        _cmd = f"python {fn_script} {subcmd}"
        try:
            with _add_path_to_environment_pythonpath(os.path.abspath(self.cwd)):
                with _switch_to_directory(self.project.path):
                    return subprocess.check_output(_cmd.split(), stderr=stderr)
        except subprocess.CalledProcessError as error:
            print(error, file=sys.stderr)
            print(error.output, file=sys.stderr)
            raise
