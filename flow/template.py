# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""This module contains the FlowProject module templates.

These templates can be initialized via the init() function defined
in this module and the main 'flow' command line interface.
"""
import os
import sys
import errno
import logging
import jinja2


logger = logging.getLogger(__name__)


TEMPLATES = {
    'minimal': [('{alias}.py', 'project_minimal.pyt'), ],
    'example': [('{alias}.py', 'project_example.pyt'), ],
    'testing': [('{alias}.py', 'project_testing.pyt'), ],
}


def init(alias=None, template=None, root=None, out=None):
    "Initialize a templated FlowProject module."
    if alias is None:
        alias = 'project'
    elif not alias.isidentifier():
        raise ValueError(
            "The alias '{}' is not a valid Python identifier and therefore "
            "not be used as a FlowProject alias.".format(alias))
    if template is None:
        template = 'minimal'
    if out is None:
        out = sys.stderr

    if os.path.splitext(alias)[1]:
        raise RuntimeError("Please provide a name without suffix!")

    project_class_name = alias.capitalize()
    if not project_class_name.endswith('Project'):
        project_class_name += 'Project'

    template_environment = jinja2.Environment(
        loader=jinja2.ChoiceLoader([
            jinja2.FileSystemLoader('templates'),
            jinja2.PackageLoader('flow', 'templates')]),
        trim_blocks=True)

    context = dict()
    context['alias'] = alias
    context['project_class_name'] = project_class_name

    # render all templates
    codes = dict()

    for fn, fn_template in TEMPLATES[template]:
        fn_ = fn.format(alias=alias)   # some of the filenames may depend on the alias
        template = template_environment.get_template(fn_template)
        codes[fn_] = template.render(** context)

    # create files
    files_created = []
    for fn, code in codes.items():
        try:
            if root is not None:
                fn = os.path.join(root, fn)
            with open(fn, 'x') as fw:
                fw.write(code + '\n')
        except OSError as e:
            if e.errno == errno.EEXIST:
                logger.error(
                    "Error while trying to initialize flow project with alias '{alias}', "
                    "a file named '{fn}' already exists!".format(alias=alias, fn=fn))
            else:
                logger.error(
                    "Error while trying to initialize flow project with alias '{alias}': "
                    "'{error}'.".format(alias=alias, error=e))
        else:
            files_created.append(fn)
            print("Created file '{}'.".format(fn), file=out)
    return files_created
