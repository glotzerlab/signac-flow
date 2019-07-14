{% extends "base_script.sh" %}
{% block header %}
#PBS -N {{ id }}
{% if walltime %}
#PBS -l walltime={{ walltime|format_timedelta }}
{% endif %}
{% if not no_copy_env %}
#PBS -V
{% endif %}
{% if memory %}
#PBS -l pmem={{ memory }}
{% endif %}
{% block tasks %}
{% set threshold = 0 if force else 0.9 %}
{% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
{% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
{% set s_gpu = ':gpus=1' if gpu_tasks else '' %}
{% set ppn = ppn|default(operations|calc_tasks('omp_num_threads', parallel, force), true) %}
{% if ppn %}
#PBS -l nodes={{ nn|default(cpu_tasks|calc_num_nodes(ppn, threshold, 'CPU'), true) }}:ppn={{ ppn }}{{ s_gpu }}
{% else %}
#PBS -l procs={{ cpu_tasks }}{{ s_gpu }}
{% endif %}
{% endblock %}
{% endblock %}
