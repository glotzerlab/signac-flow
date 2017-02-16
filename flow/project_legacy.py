"""This module provides legacy functions for backwards-compatibility."""
import io
import datetime
import logging
from itertools import islice

from . import manage


logger = logging.getLogger(__name__)


class JobScript(io.StringIO):
    "Simple StringIO wrapper to implement cmd wrapping logic."

    def writeline(self, line, eol='\n'):
        "Write one line to the job script."
        self.write(line + eol)

    def write_cmd(self, cmd, parallel=False, np=1, mpi_cmd=None, **kwargs):
        """Write a command to the jobscript.

        This command wrapper function is a convenience function, which
        adds mpi and other directives whenever necessary.

        The ``mpi_cmd`` argument should be a callable, with the following
        signature: ``mpi_cmd(cmd, np, **kwargs)``.

        :param cmd: The command to write to the jobscript.
        :type cmd: str
        :param parallel: Commands should be executed in parallel.
        :type parallel: bool
        :param np: The number of processors required for execution.
        :type np: int
        :param mpi_cmd: MPI command wrapper.
        :type mpi_cmd: callable
        :param kwargs: All other forwarded parameters."""
        if np > 1:
            if mpi_cmd is None:
                raise RuntimeError("Requires mpi_cmd wrapper.")
            cmd = mpi_cmd(cmd, np=np)
        if parallel:
            cmd += ' &'
        self.writeline(cmd)
        return np


def _submit(self, scheduler, to_submit, pretend,
            serial, bundle, after, walltime, **kwargs):
    "Submit jobs to the scheduler."
    script = JobScript()
    self.write_header(
        script=script, walltime=walltime, serial=serial,
        bundle=bundle, after=after, ** kwargs)
    jobids_bundled = []
    np_total = 0
    for job, operation in to_submit:
        jobsid = _get_jobsid(job, operation)

        def set_status(value):
            "Update the job's status dictionary."
            status_doc = job.document.get('status', dict())
            status_doc[jobsid] = int(value)
            job.document['status'] = status_doc
            return int(value)

        np = self.write_user(
            script=script, job=job, operation=operation,
            parallel=not serial and bundle is not None, **kwargs)
        if np is None:
            raise RuntimeError(
                "Failed to return 'num_procs' value in write_user()!")
        np_total = max(np, np_total) if serial else np_total + np
        if pretend:
            set_status(manage.JobStatus.registered)
        else:
            set_status(manage.JobStatus.submitted)
        jobids_bundled.append(jobsid)
    script.write('wait')
    script.seek(0)
    if not len(jobids_bundled):
        return False

    if len(jobids_bundled) > 1:
        sid = self._store_bundled(jobids_bundled)
    else:
        sid = jobsid
    scheduler_job_id = scheduler.submit(
        script=script, jobsid=sid,
        np=np_total, walltime=walltime, pretend=pretend, **kwargs)
    logger.info("Submitted {}.".format(sid))
    if serial and not bundle:
        if after is None:
            after = ''
        after = ':'.join(after.split(':') + [scheduler_job_id])
    return True


def _get_jobsid(job, operation):
    "Return a unique job session id based on the job and operation."
    return '{jobid}-{operation}'.format(jobid=job, operation=operation)


def _blocked(self, job, operation, **kwargs):
    "Check if job, operation combination is blocked for scheduling."
    try:
        status = job.document['status'][_get_jobsid(job, operation)]
        return status >= manage.JobStatus.submitted
    except KeyError:
        return False


def _eligible(self, job, operation=None, **kwargs):
    """Internal check for the job's eligible for operation.

    A job is only eligible if the public :meth:`~.eligible` method
    returns True and the job is not blocked by the scheduler.

    :raises RuntimeError: If the public eligible method returns None."""
    ret = self.eligible(job, operation, **kwargs) \
        and not _blocked(self, job, operation, **kwargs)
    if ret is None:
        raise RuntimeError("Unable to determine eligiblity for job '{}' "
                           "and job type '{}'.".format(job, operation))
    return ret


def filter_non_eligible(self, to_submit, **kwargs):
    "Return only those jobs for submittal, which are eligible."
    return ((j, jt) for j, jt in to_submit
            if _eligible(self, j, jt, **kwargs))


def to_submit(self, job_ids=None, operation=None, job_filter=None):
    """Generate a sequence of (job_id, operation) value pairs for submission.

    :param job_ids: A list of job_id's,
        defaults to all jobs found in the workspace.
    :param operation: A specific operation,
        defaults to the result of :meth:`~.next_operation`.
    :param job_filter: A JSON encoded filter,
        that all jobs to be submitted need to match."""
    if job_ids is None:
        jobs = list(self.find_jobs(job_filter))
    else:
        jobs = [self.open_job(id=jobid) for jobid in job_ids]
    if operation is None:
        operations = (self.next_operation(job) for job in jobs)
    else:
        operations = [operation] * len(jobs)
    return zip(jobs, operations)


def submit_jobs(self, scheduler, to_submit, walltime=None,
                bundle=None, serial=False, after=None,
                num=None, pretend=False, force=False, **kwargs):
    """Submit jobs to the scheduler.

    :param scheduler: The scheduler instance.
    :type scheduler: :class:`~.flow.manage.Scheduler`
    :param to_submit: A sequence of (job_id, operation) tuples.
    :param walltime: The maximum wallclock time in hours.
    :type walltime: float
    :param bundle: Bundle up to 'bundle' number of jobs during submission.
    :type bundle: int
    :param serial: Schedule jobs in serial or execute bundled
        jobs in serial.
    :type serial: bool
    :param after: Execute all jobs after the completion of the job operation
        with this job session id. Implementation is scheduler dependent.
    :type after: str
    :param num: Do not submit more than 'num' jobs to the scheduler.
    :type num: int
    :param pretend: Do not actually submit, but instruct the scheduler
        to pretend scheduling.
    :type pretend: bool
    :param force: Ignore all eligibility checks, just submit.
    :type force: bool
    :param kwargs: Other keyword arguments which are forwareded."""
    if walltime is not None:
        walltime = datetime.timedelta(hours=walltime)
    if not force:
        to_submit = filter_non_eligible(self, to_submit, **kwargs)
    to_submit = islice(to_submit, num)
    if bundle is not None:
        n = None if bundle == 0 else bundle
        while True:
            ts = islice(to_submit, n)
            if not _submit(self, scheduler, ts, walltime=walltime,
                           bundle=bundle, serial=serial, after=after,
                           num=num, pretend=pretend, force=force, **kwargs):
                break
    else:
        for ts in to_submit:
            _submit(self, scheduler, [ts], walltime=walltime,
                    bundle=bundle, serial=serial, after=after,
                    num=num, pretend=pretend, force=force, **kwargs)


def submit(self, scheduler, job_ids=None,
           operation=None, job_filter=None, **kwargs):
    """Wrapper for :meth:`~.to_submit` and :meth:`~.submit_jobs`.

    This function passes the return value of :meth:`~.to_submit`
    to :meth:`~.submit_jobs`.

    :param scheduler: The scheduler instance.
    :type scheduler: :class:`~.flow.manage.Scheduler`
    :param job_ids: A list of job_id's,
        defaults to all jobs found in the workspace.
    :param operation: A specific operation,
        defaults to the result of :meth:`~.next_operation`.
    :param job_filter: A JSON encoded filter that all jobs
        to be submitted need to match.
    :param kwargs: All other keyword arguments are forwarded
        to :meth:`~.submit_jobs`."""
    if job_ids is not None and not len(job_ids):
        job_ids = None
    return submit_jobs(self,
                       scheduler=scheduler,
                       to_submit=to_submit(
                           self, job_ids, operation, job_filter),
                       **kwargs)
