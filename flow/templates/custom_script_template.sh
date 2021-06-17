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
{% block body %}
    {{- super () -}}
{% endblock body %}
{% endraw %}
