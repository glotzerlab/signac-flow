{# Templated in accordance with: https://www.olcf.ornl.gov/for-users/system-user-guides/summit/running-jobs/ #}
{% extends "lsf.sh" %}
{% block tasks %}
{% set threshold = 0 if force else 0.9 %}
{% set nn = operations|map('guess_resource_sets')|calc_num_nodes %}
#BSUB -nnodes {{ nn }}
{% endblock %}
{% block header %}
{{ super() -}}
{% set account = account|default(environment|get_account_name, true) %}
{% if account %}
#BSUB -P {{ account }}
{% endif %}
{% endblock %}
{% block body %}
{% set cmd_suffix = cmd_suffix|default('') ~ (' &' if parallel else '') %}
{% for operation in operations %}
{% set mpi_prefix =  operation|get_mpi_prefix %}

# {{ "%s"|format(operation) }}
{% if operation.directives.omp_num_threads %}
export OMP_NUM_THREADS={{ operation.directives.omp_num_threads }}
{% endif %}
{{ mpi_prefix }}{{ cmd_prefix }}{{ operation.cmd }}{{ cmd_suffix }}
{% endfor %}
{% endblock %}
