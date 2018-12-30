{# Templated in accordance with: https://www.psc.edu/bridges/user-guide #}
{# This template can only be used with P100 GPUs! #}
{% set mpiexec = "mpirun" %}
{% extends "slurm.sh" %}
{% block tasks %}
{% set threshold = 0 if force else 0.9 %}
{% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
{% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
{% if gpu_tasks %}
{% if not ('GPU' in partition or force) %}
{% raise "GPU-operations require a GPU partition!" %}
{% endif %}
{% set nn_cpu = cpu_tasks|calc_num_nodes(32) %}
{% set nn_gpu = gpu_tasks|calc_num_nodes(2) %}
{% set nn = nn|default((nn_cpu, nn_gpu)|max, true) %}
{% else %}
{% if 'GPU' in partition and not force %}
{% raise "Requesting GPU partition, but no GPUs requested!" %}
{% endif %}
{% set nn = nn|default(cpu_tasks|calc_num_nodes(32), true) %}
{% endif %}
{% if partition in ('GPU', 'GPU-small') %}
#SBATCH -N {{ nn|check_utilization(gpu_tasks, 2, threshold, 'GPU') }}
#SBATCH --ntasks-per-node=32
#SBATCH --gres=gpu:p100:2
{% elif partition == 'GPU-shared' %}
#SBATCH -N {{ nn|default(1, true)|check_utilization(gpu_tasks, 1, threshold, 'GPU') }}
#SBATCH --ntasks-per-node=16
{% elif 'shared' in partition %}
#SBATCH -N {{ nn|default(1, true) }}
#SBATCH --ntasks-per-node={{ cpu_tasks }}
{% else %}
#SBATCH -N {{nn|check_utilization(cpu_tasks, 28, threshold, 'CPU') }}
#SBATCH --ntasks-per-node={{ (28, cpu_tasks)|min }}
{% endif %}
{% endblock  tasks %}
{% block header %}
{{ super() -}}
{% set account = account|default(environment|get_account_name, true) %}
{% if account %}
#SBATCH -A {{ account }}
{% endif %}
{% if memory %}
#SBATCH --mem={{ memory }}G
{% endif %}
{% endblock header %}
