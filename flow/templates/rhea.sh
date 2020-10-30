{% extends "slurm.sh" %}
{% block tasks %}
{% set threshold = 0 if force else 0.9 %}
{% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
{% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
{% if gpu_tasks and 'gpu' not in partition and not force %}
{% raise "Requesting GPUs requires a gpu partition!" %}
{% endif %}
{% set nn_cpu = cpu_tasks|calc_num_nodes(16) %}
{% set nn_gpu = gpu_tasks|calc_num_nodes(2) if 'gpu' in partition else 0 %}
{% set nn = nn|default((nn_cpu, nn_gpu)|max, true) %}
{% if partition == 'gpu' %}
#SBATCH --nodes={{ nn|check_utilization(gpu_tasks, 2, threshold, 'GPU') }}
{% else %}{# standard compute partition #}
#SBATCH --nodes={{ nn|check_utilization(cpu_tasks, 16, threshold, 'CPU') }}
{% endif %}
{% endblock tasks %}
{% block header %}
{{ super () -}}
{% set account = account|default(environment|get_account_name, true) %}
{% if account %}
#SBATCH -A {{ account }}
{% endif %}
{% endblock %}
