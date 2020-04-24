#!/usr/bin/env python
"""Generate a project with specific set of operations and pretend to submit."""
import os
import io
from itertools import count
from contextlib import redirect_stdout, redirect_stderr

import click
import signac
import flow
import jinja2


def add_operation(Project, name):
    click.echo("Specify directives for the next operation.")
    nranks = click.prompt("nranks", default=0)
    omp_num_threads = click.prompt('omp_num_threads', default=0)
    np = click.prompt("np", default=max(1, nranks) * max(1, omp_num_threads))
    ngpu = click.prompt("ngpus", default=0)

    @flow.directives(np=np)
    @flow.directives(nranks=nranks)
    @flow.directives(omp_num_threads=omp_num_threads)
    @flow.directives(ngpu=ngpu)
    def op(job):
        pass

    op.__name__ = name
    Project.operation(op)

    return click.prompt("Do you want to add more operations?", default=False)


@click.command()
@click.option('-n', '--num-jobs', default=1, show_default=True,
              help="The number of signac jobs to initialize.")
@click.option('-b', '--bundle', default=1, show_default=True,
              help="Specify the bundle size.")
@click.option('-p', '--parallel', is_flag=True,
              help="Specify whether bundles are to be executed in parallel.")
@click.option('-e', '--entrypoint', default='/path/to/project.py',
              help="Entry point path used in output scripts.")
def cli(num_jobs, bundle, parallel, entrypoint):
    """Generate a project with specific set of operations and pretend to submit.

    Usage example:

        \b ./interactive_template_test.py -n 28 -b 28 -p

    You can use the `SIGNAC_FLOW_ENVIRONMENT` environment variable to specify
    the environment to test, e.g.:

        \b
        $ SIGNAC_FLOW_ENVIRONMENT=BridgesEnvironment ./interactive_template_test.py

    See `./interactive_template_test.py --help` for more information.
    """
    import flow.environments

    class Project(flow.FlowProject):
        pass

    for i in count():
        if not add_operation(Project, 'op_{}'.format(i)):
            break

    with signac.TemporaryProject() as tmp_project:
        for i in range(num_jobs):
            tmp_project.open_job(dict(foo=i)).init()
        flow_project = Project.get_project(root=tmp_project.root_directory())
        flow_project._entrypoint.setdefault('path', entrypoint)

        partition = ''
        force = False
        while True:
            if force:
                click.echo("Pretend submit with --force.")
            partition = click.prompt('Partition (Hit CTRL-C to cancel.)',
                                     default=partition, show_default=True)
            try:
                out = io.StringIO()
                with redirect_stdout(out):
                    with redirect_stderr(open(os.devnull, 'w')):
                        flow_project.submit(
                            pretend=True, partition=partition,
                            bundle_size=bundle, parallel=parallel,
                            force=force)
                click.echo_via_pager(out.getvalue())
            except (jinja2.exceptions.TemplateError, RuntimeError) as error:
                click.secho(str(error), fg='yellow')
                force = click.prompt('Use --force?', default=False)
            else:
                force = False


if __name__ == '__main__':
    cli()
