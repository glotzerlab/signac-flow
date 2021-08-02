{#
This is a jinja template used to generate another jinja template.
This file is used by `flow template create` to detect the current environment
and create a new script template that can be modified by the user to add
custom commands (e.g. `module load ...`).
#}
{{ '{% extends "' + extend_template + '" %}' }}
{% raw %}
{% block header %}
    {{- super () -}}
{% endblock header %}
{% block custom_content %}
{#
    This block is not used by any other template and can be safely modified
    without the need to call super(). We recommend most additions to the
    templates go here if they are not direct changes to an existing template.

    For example, commands like `module load ...` or printing diagnostic
    information from the scheduler can be done in this block.
#}
{% endblock custom_content %}
{% block body %}
    {{- super () -}}
{% endblock body %}
{% endraw %}
