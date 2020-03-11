{# Templated in accordance with: http://www.sdsc.edu/support/user_guides/comet.html#running #}
{# This template can only be used with P100 GPUs! #}
{% extends "slurm.sh" %}
{% block tasks %}
{% set threshold = 0 if force else 0.9 %}
{% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
{% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
{% if gpu_tasks and 'gpu' not in partition and not force %}
{% raise "Requesting GPUs requires a gpu partition!" %}
{% endif %}
{% set nn_cpu = cpu_tasks|calc_num_nodes(24) %}
{% set nn_gpu = gpu_tasks|calc_num_nodes(4) if 'gpu' in partition else 0 %}
{% set nn = nn|default((nn_cpu, nn_gpu)|max, true) %}
{% if partition == 'gpu' %}
#SBATCH --nodes={{ nn|check_utilization(gpu_tasks, 4, threshold, 'GPU') }}
#SBATCH --gres=gpu:p100:{{ (gpu_tasks, 4)|min }}
{% elif partition == 'gpu-shared' %}
#SBATCH --nodes={{ nn|default(1, true)|check_utilization(gpu_tasks, 1, threshold, 'GPU') }}
#SBATCH --ntasks-per-node={{ (gpu_tasks * 7, cpu_tasks)|max }}
#SBATCH --gres=gpu:p100:{{ gpu_tasks }}
{% elif 'shared' in partition %}{# standard shared partition #}
#SBATCH --nodes={{ nn|default(1, true) }}
#SBATCH --ntasks-per-node={{ cpu_tasks }}
{% else %}{# standard compute partition #}
#SBATCH --nodes={{ nn|check_utilization(cpu_tasks, 24, threshold, 'CPU') }}
#SBATCH --ntasks-per-node={{ (24, cpu_tasks)|min }}
{% endif %}
{% endblock tasks %}
{% block header %}
{{ super () -}}
{% set account = account|default(environment|get_account_name, true) %}
{% if account %}
#SBATCH -A {{ account }}
{% endif %}
{% endblock %}
