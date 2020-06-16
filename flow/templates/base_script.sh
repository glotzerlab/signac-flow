{# The following variables are available to all scripts. #}
{% if parallel %}
{% set np_global = operations|map(attribute='directives.np')|sum %}
{% else %}
{% set np_global = operations|map(attribute='directives.np')|max %}
{% endif %}
{% block header %}
{% endblock %}

{% block project_header %}
set -e
set -u

cd {{ project.config.project_dir }}
{% endblock %}
{% block body %}
{% set cmd_suffix = cmd_suffix|default('') ~ (' &' if parallel else '') %}
{% for operation in operations %}

# {{ "%s"|format(operation) }}
{{ operation.cmd }}{{ cmd_suffix }}
{% if operation.eligible_operations|length > 0 %}
# Eligible to run:
{% for run_op in operation.eligible_operations %}
# {{ run_op.cmd }}
{% endfor %}
{% endif %}
{% if operation.operations_with_unmet_preconditions|length > 0 %}
# Operations with unmet preconditions:
{% for run_op in operation.operations_with_unmet_preconditions %}
# {{ run_op.cmd }}
{% endfor %}
{% endif %}
{% if operation.operations_with_met_postconditions|length > 0 %}
# Operations with all postconditions met:
{% for run_op in operation.operations_with_met_postconditions %}
# {{ run_op.cmd }}
{% endfor %}
{% endif %}
{% endfor %}
{% endblock %}
{% block footer %}
{% if parallel %}
wait
{% endif %}
{% endblock %}
