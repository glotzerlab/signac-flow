# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""FlowProject module templates.

These templates can be initialized via the init() function defined
in this module and the main 'flow' command line interface.
"""
import errno
import logging
import os
import sys

import jinja2

logger = logging.getLogger(__name__)


TEMPLATES = {
    "minimal": [
        ("{alias}.py", "project_minimal.pyt"),
    ],
    "example": [
        ("{alias}.py", "project_example.pyt"),
    ],
    "testing": [
        ("{alias}.py", "project_testing.pyt"),
    ],
}


def init(alias=None, template=None, root=None):
    """Initialize a templated :class:`~.FlowProject` module.

    Parameters
    ----------
    alias : str
        Python identifier used as a file name for the template output. Uses
        ``"project"`` if None.  (Default value = None)

    template : str
        Name of the template to use. Built-in templates are:

        * ``"minimal"``
        * ``"example"``
        * ``"testing"``

        Uses ``"minimal"`` if None. (Default value = None)

    root : str
        Directory where the output file is placed. Uses the current working
        directory if None. (Default value = None)

    Returns
    -------
    list
        List of file names created.

    """
    if alias is None:
        alias = "project"
    elif not alias.isidentifier():
        raise ValueError(
            f"The alias '{alias}' is not a valid Python identifier and therefore "
            "cannot be used as a FlowProject alias."
        )
    if template is None:
        template = "minimal"

    if os.path.splitext(alias)[1]:
        raise RuntimeError("Please provide a name without suffix!")

    project_class_name = alias.capitalize()
    if not project_class_name.endswith("Project"):
        project_class_name += "Project"

    template_environment = jinja2.Environment(
        loader=jinja2.ChoiceLoader(
            [
                jinja2.FileSystemLoader("templates"),
                jinja2.PackageLoader("flow", "templates"),
            ]
        ),
        trim_blocks=True,
        lstrip_blocks=True,
    )

    context = {}
    context["alias"] = alias
    context["project_class_name"] = project_class_name

    # render all templates
    codes = {}

    for fn, fn_template in TEMPLATES[template]:
        fn_ = fn.format(alias=alias)  # some of the filenames may depend on the alias
        template = template_environment.get_template(fn_template)
        codes[fn_] = template.render(**context)

    # create files
    files_created = []
    for fn, code in codes.items():
        try:
            if root is not None:
                fn = os.path.join(root, fn)
            with open(fn, "x") as fw:
                fw.write(code + "\n")
        except OSError as error:
            error_message = (
                f"Error while creating FlowProject module with name '{alias}': "
            )
            if error.errno == errno.EEXIST:
                error_message += f"a file named '{fn}' already exists!"
            else:
                error_message += f"'{error}'."
            raise OSError(error_message)
        else:
            files_created.append(fn)
            print(f"Created file '{fn}'.", file=sys.stderr)
    return files_created
