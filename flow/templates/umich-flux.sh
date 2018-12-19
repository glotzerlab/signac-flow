{# Templated in accordance with: https://arc-ts.umich.edu/flux-user-guide/ #}
{% extends "torque.sh" %}
{% block tasks %}
{% set threshold = 0 if force else 0.9 %}
{% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
{% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
{% set ppn = ppn|default(operations|calc_tasks('omp_num_threads', parallel, force), true) %}
{% set s_ppn = ':ppn=' ~ ppn if ppn else '' %}
{% set nn_cpu = cpu_tasks|calc_num_nodes(ppn or 1, threshold, 'CPU') %}
{% set nn_gpu = gpu_tasks|calc_num_nodes(1, threshold, 'GPU') %}
{% if mode == 'gpu' or gpu_tasks %}
#PBS -l nodes={{ (nn_cpu, nn_gpu)|max }}{{ s_ppn }}:gpus=1
{% elif ppn %}
#PBS -l nodes={{ (nn_cpu, nn_gpu)|max }}{{ s_ppn }}
{% else %}
#PBS -l procs={{ cpu_tasks }}
{% endif %}
{% endblock %}

{% block header %}
{{ super() -}}
{% set account = account|default(environment|get_account_name, true) %}
{% if account %}
#PBS -A {{ account }}
{% endif %}
{% set qos = 'qos'|get_config_value(environment, 'flux') %}
#PBS -l qos={{ qos }}
{% set queue = 'cpu_queue'|get_config_value(environment, 'flux') %}
{% if mode == 'gpu' or gpu_tasks %}
{% set queue = queue+'g' %}
{% endif %}
#PBS -q {{ queue }}
{% endblock %}
