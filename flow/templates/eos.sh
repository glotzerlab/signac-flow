{# Templated in accordance with: https://www.olcf.ornl.gov/for-users/system-user-guides/eos/running-jobs/ #}
{% set mpiexec = "aprun" %}
{% extends "torque.sh" %}
{% block tasks %}
{% set threshold = 0 if force else 0.9 %}
{% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
{% if operations|calc_tasks('ngpu', true, true) and not force %}
{% raise "GPUs were requested but are unsupported by Eos!" %}
{% endif %}
{% set nn = nn|default(cpu_tasks|calc_num_nodes(24), true) %}
#PBS -l nodes={{ nn|check_utilization(cpu_tasks, 24, threshold, 'CPU') }}
{% endblock %}
{% block header %}
{{ super() -}}
{% set account = account|default(environment|get_account_name, true) %}
{% if account %}
#PBS -A {{ account }}
{% endif %}
{% endblock %}
