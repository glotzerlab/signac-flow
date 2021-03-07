# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Initialize a project for testing."""
import signac

from .template import init


def make_project(alias="project", root=None, **kwargs):
    r"""Initialize a project for testing.

    The initialized project has a few operations and a few jobs that are in
    various points in the workflow defined by the project.

    Parameters
    ----------
    alias : str
        Python identifier used as a file name for the template output.
        (Default value = ``"project"``)
    root : str
        Directory where the output file is placed. Uses the current working
        directory if None. (Default value = None)
    \*\*kwargs
        Keyword arguments forwarded to :meth:`signac.testing.init_jobs`.

    Returns
    -------
    :class:`signac.Project`
        Project with initialized jobs.

    """
    init(alias=alias, root=root, template="testing")
    project = signac.init_project(name=alias, root=root)
    signac.testing.init_jobs(project, **kwargs)
    return project
