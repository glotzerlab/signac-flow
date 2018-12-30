{# Templated in accordance with: https://www.olcf.ornl.gov/for-users/system-user-guides/titan/running-jobs/ #}
{% set mpiexec = "aprun" %}
{% extends "torque.sh" %}
{% block tasks %}
{% set threshold = 0 if force else 0.9 %}
{% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
{% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
{% set nn_cpu = cpu_tasks|calc_num_nodes(16) %}
{% set nn_gpu = gpu_tasks|calc_num_nodes(1) %}
{% set nn = nn|default((nn_cpu, nn_gpu)|max, true) %}
#PBS -l nodes={{ nn|check_utilization(gpu_tasks, 1, threshold, 'GPU') }}
{% endblock %}
{% block header %}
{{ super() -}}
{% set account = account|default(environment|get_account_name, true) %}
{% if account %}
#PBS -A {{ account }}
{% endif %}
{% endblock %}
