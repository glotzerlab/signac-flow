# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""This module contains the FlowProject module templates.

These templates can be initialized via the init() function defined
in this module and the main 'flow' command line interface.
"""
from __future__ import print_function
import os
import sys
import errno
import logging

try:
    import jinja2
    from jinja2 import TemplateNotFound as Jinja2TemplateNotFound
except ImportError:
    # Mock exception, which will never be raised.
    class Jinja2TemplateNotFound(Exception):
        pass

    JINJA2 = False
else:
    JINJA2 = True

from .util.misc import _is_identifier


logger = logging.getLogger(__name__)


TEMPLATES = {
    'minimal': [('{alias}.py', 'project_minimal.pyt'), ],
    'example': [('{alias}.py', 'project_example.pyt'), ],
    'testing': [('{alias}.py', 'project_testing.pyt'), ],
}


def init(alias=None, template=None, root=None, out=None):
    "Initialize a templated FlowProject module."
    if not JINJA2:
        raise ValueError("The init() function requires the 'jinja2' package.")

    if alias is None:
        alias = 'project'
    elif not _is_identifier(alias):
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
