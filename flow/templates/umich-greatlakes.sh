{% extends "slurm.sh" %}
{% set partition = partition|default('standard', true) %}
{% block tasks %}
    {% set threshold = 0 if force else 0.9 %}
    {% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
    {% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
    {% if gpu_tasks and 'gpu' not in partition and not force %}
        {% raise "Requesting GPUs requires a gpu partition!" %}
    {% endif %}
    {% set nn_cpu = cpu_tasks|calc_num_nodes(36) if 'gpu' not in partition else cpu_tasks|calc_num_nodes(40) %}
    {% set nn_gpu = gpu_tasks|calc_num_nodes(2) if 'gpu' in partition else 0 %}
    {% set nn = nn|default((nn_cpu, nn_gpu)|max, true) %}
    {% if partition == 'gpu' %}
#SBATCH --nodes={{ nn|default(1, true) }}
#SBATCH --ntasks-per-node={{ (gpu_tasks, cpu_tasks)|max }}
#SBATCH --gpus={{ gpu_tasks }}
    {% else %}{# standard compute partition #}
#SBATCH --nodes={{ nn }}
#SBATCH --ntasks-per-node={{ (36, cpu_tasks)|min }}
    {% endif %}
{% endblock tasks %}
{% block header %}
    {{- super () -}}
    {% set account = account|default(environment|get_account_name, true) %}
    {% if account %}
#SBATCH --account={{ account }}
    {% endif %}
{% endblock header %}
