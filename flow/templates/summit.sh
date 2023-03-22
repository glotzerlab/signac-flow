{# Templated in accordance with: https://www.olcf.ornl.gov/for-users/system-user-guides/summit/running-jobs/ #}
{% extends "lsf.sh" %}
{% block tasks %}
#BSUB -nnodes {{ resources.num_nodes }}
{% endblock tasks %}
{% block header %}
    {{- super() -}}
    {% set account = account|default(project|get_account_name, true) %}
    {% if account %}
#BSUB -P {{ account }}
    {% endif %}
{% endblock header %}
