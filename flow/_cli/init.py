import os

from .template import TEMPLATE


def init(alias):
    "Initialize a templated FlowProject workflow module."
    if alias is None:
        alias = 'project'

    if not alias.endswith('.py'):
        alias = alias + '.py'

    if os.path.isfile(alias):
        raise RuntimeError(
            "Error: flow project named {!r} already exists in current "
            "directory! Please choose a different name to continue.".format(alias))

    project_class_name = os.path.splitext(alias)[0].capitalize()
    if not project_class_name.endswith('Project'):
        project_class_name += 'Project'

    for fn, template in TEMPLATE.items():
        with open(alias, 'x') as fw:
            fw.write(template.format(project_class=project_class_name))
