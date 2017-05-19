import os

from .template import TEMPLATES


def init(alias=None, template=None):
    "Initialize a templated FlowProject workflow module."
    if alias is None:
        alias = 'project'
    if template is None:
        template = 'minimal'

    if os.path.splitext(alias)[1]:
        raise RuntimeError("Please provide a name without suffix!")

    project_class_name = alias.capitalize()
    if not project_class_name.endswith('Project'):
        project_class_name += 'Project'

    for fn, code in TEMPLATES[template].items():
        try:
            fn_ = fn.format(alias=alias)   # some of the filenames may depend on the alias
            with open(fn_, 'x') as fw:
                fw.write(code.format(alias=alias, project_class=project_class_name))
        except OSError as e:
            raise RuntimeError(
                "Error while trying to initialize flow project with alias '{alias}', a file named "
                "'{fn}' already exists! Please choose a different name to continue.".format(
                    alias=alias, fn=fn_))
