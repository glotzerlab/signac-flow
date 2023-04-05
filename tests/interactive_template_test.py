#!/usr/bin/env python
"""Generate a project with specific set of operations and pretend to submit."""
import io
import os
from contextlib import redirect_stderr, redirect_stdout
from itertools import count

import click
import jinja2
import signac


def add_operation(Project, name):
    click.echo("Specify directives for the next operation.")
    directives = {}
    directives["nranks"] = click.prompt("nranks", default=0)
    directives["omp_num_threads"] = click.prompt("omp_num_threads", default=0)
    directives["np"] = click.prompt(
        "np",
        default=max(1, directives["nranks"]) * max(1, directives["omp_num_threads"]),
    )
    directives["ngpu"] = click.prompt("ngpus", default=0)

    def op(job):
        pass

    op.__name__ = name
    Project.operation(op, directives=directives)

    return click.prompt("Do you want to add more operations?", default=False)


@click.command()
@click.option(
    "-n",
    "--num-jobs",
    default=1,
    show_default=True,
    help="The number of signac jobs to initialize.",
)
@click.option(
    "-b", "--bundle", default=1, show_default=True, help="Specify the bundle size."
)
@click.option(
    "-p",
    "--parallel",
    is_flag=True,
    help="Specify whether bundles are to be executed in parallel.",
)
@click.option(
    "-e",
    "--entrypoint",
    default="/path/to/project.py",
    help="Entry point path used in output scripts.",
)
def cli(num_jobs, bundle, parallel, entrypoint):
    """Generate a project with specific set of operations and pretend to submit.

    Usage example:

        \b ./interactive_template_test.py -n 28 -b 28 -p

    You can use the `SIGNAC_FLOW_ENVIRONMENT` environment variable to specify
    the environment to test, e.g.:

        \b
        $ SIGNAC_FLOW_ENVIRONMENT=Bridges2Environment ./interactive_template_test.py

    See `./interactive_template_test.py --help` for more information.
    """
    import flow.environments

    class Project(flow.FlowProject):
        pass

    for i in count():
        if not add_operation(Project, f"op_{i}"):
            break

    with signac.TemporaryProject() as tmp_project:
        for i in range(num_jobs):
            tmp_project.open_job(dict(foo=i)).init()
        flow_project = Project.get_project(path=tmp_project.path)
        flow_project._entrypoint.setdefault("path", entrypoint)

        partition = ""
        force = False
        while True:
            if force:
                click.echo("Pretend submit with --force.")
            partition = click.prompt(
                "Partition (Hit CTRL-C to cancel.)",
                default=partition,
                show_default=True,
            )
            try:
                out = io.StringIO()
                with redirect_stdout(out):
                    with redirect_stderr(open(os.devnull, "w")):
                        flow_project.submit(
                            pretend=True,
                            partition=partition,
                            bundle_size=bundle,
                            parallel=parallel,
                            force=force,
                        )
                click.echo_via_pager(out.getvalue())
            except (jinja2.exceptions.TemplateError, RuntimeError) as error:
                click.secho(str(error), fg="yellow")
                force = click.prompt("Use --force?", default=False)
            else:
                force = False


if __name__ == "__main__":
    cli()
