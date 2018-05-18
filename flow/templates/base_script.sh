{# Number of tasks is the same for any script type #}
{% if parallel %}
{% set num_tasks = operations|map(attribute='directives.np')|sum %}
{% else %}
{% set num_tasks = operations|map(attribute='directives.np')|max %}
{% endif %}
{% block header %}
{% endblock %}

{% block project_header %}
set -e
set -u

cd {{ project.config.project_dir }}
{% endblock %}

{% block body %}
{% for operation in operations %}
# Operation '{{ operation.name }}' for job '{{ operation.job._id }}':
{% if parallel %}
{{ prefix_cmd }}{{ operation.cmd }} &
{% else %}
{{ prefix_cmd }}{{ operation.cmd }}
{% endif %}
{% endfor %}
{% endblock %}
{% block footer %}
{% if parallel %}
wait
{% endif %}
{% endblock %}
