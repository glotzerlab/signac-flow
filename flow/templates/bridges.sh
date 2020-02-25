{# Templated in accordance with: https://www.psc.edu/bridges/user-guide #}
{# This template can only be used with P100 GPUs on GPU, or V100 GPUs on GPU-AI. #}
{% extends "slurm.sh" %}
{% block tasks %}
{% set threshold = 0 if force else 0.9 %}
{% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
{% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
{% if gpu_tasks %}
{% if not ('GPU' in partition or force) %}
{% raise "GPU operations require a GPU partition!" %}
{% endif %}
{% if partition == 'GPU-AI' %}
{# GPU-AI nodes with V100s #}
{% set nn_cpu = cpu_tasks|calc_num_nodes(40) %}
{% set nn_gpu = gpu_tasks|calc_num_nodes(8) %}
{% set nn = nn|default((nn_cpu, nn_gpu)|max, true) %}
{% else %}
{# GPU nodes with P100s #}
{% set nn_cpu = cpu_tasks|calc_num_nodes(32) %}
{% set nn_gpu = gpu_tasks|calc_num_nodes(2) %}
{% set nn = nn|default((nn_cpu, nn_gpu)|max, true) %}
{% endif %}
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
{% if gpu_tasks > 1 and gpu_tasks % 2 %}
{% raise "Can only request multiples of two GPUs when submitting to the GPU-shared partition." %}
{% endif %}
#SBATCH -N {{ nn|default(1, true)|check_utilization(gpu_tasks, 1, threshold, 'GPU') }}
#SBATCH --ntasks-per-node=16
#SBATCH --gres=gpu:p100:{{ 2 if gpu_tasks > 1 else 1 }}
{% elif partition == 'GPU-AI' %}
{% set gpus_per_node = (gpu_tasks / nn)|round(0, 'ceil')|int %}
{% set cpus_per_node = (cpu_tasks / nn)|round(0, 'ceil')|int %}
{% if cpus_per_node > gpus_per_node * 5 and not force %}
{% raise "Cannot request more than 5 CPUs per GPU." %}
{% endif %}
#SBATCH -N {{ nn|default(1, true)|check_utilization(gpu_tasks, 8, threshold, 'GPU') }}
#SBATCH --ntasks-per-node={{ cpus_per_node }}
#SBATCH --gres=gpu:volta16:{{ gpus_per_node }}
{% elif 'shared' in partition %}
#SBATCH -N {{ nn|default(1, true) }}
#SBATCH --ntasks-per-node={{ cpu_tasks }}
{% else %}
#SBATCH -N {{ nn|check_utilization(cpu_tasks, 28, threshold, 'CPU') }}
#SBATCH --ntasks-per-node={{ (28, cpu_tasks)|min }}
{% endif %}
{% endblock tasks %}
{% block header %}
{{ super() -}}
{% set account = account|default(environment|get_account_name, true) %}
{% if account %}
#SBATCH -A {{ account }}
{% endif %}
{% endblock header %}
