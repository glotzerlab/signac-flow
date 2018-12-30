{# Templated in accordance with: https://www.olcf.ornl.gov/for-users/system-user-guides/summit/running-jobs/ #}
{% set mpiexec = "jsrun" %}
{% extends "lsf.sh" %}
{% set cores_per_node = 42 %}
{% set gpus_per_node = 6 %}
{% block tasks %}
{% set threshold = 0 if force else 0.9 %}
{% set nn = operations|map('guess_resource_sets', cores_per_node, gpus_per_node)|calc_num_nodes(cores_per_node, gpus_per_node) %}
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
{% set mpi_prefix = "jsrun " ~ operation|guess_resource_sets(cores_per_node, gpus_per_node)|jsrun_options ~ " -d packed -b rs " %}

# {{ "%s"|format(operation) }}
{% if operation.directives.omp_num_threads %}
export OMP_NUM_THREADS={{ operation.directives.omp_num_threads }}
{% endif %}
{{ mpi_prefix }}{{ cmd_prefix }}{{ operation.cmd }}{{ cmd_suffix }}
{% endfor %}
{% endblock %}
