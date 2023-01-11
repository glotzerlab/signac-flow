{% extends "slurm.sh" %}
{% set partition = partition|default('standard', true) %}
{% block tasks %}
    {% set threshold = 0 if force else 0.9 %}
    {% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
    {% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
    {% if gpu_tasks and 'gpu' not in partition and not force %}
        {% raise "Requesting GPUs requires a gpu partition!" %}
    {% endif %}
    {% set nn_cpu = cpu_tasks|calc_num_nodes(48) if 'gpu' not in partition else cpu_tasks|calc_num_nodes(48) %}
    {% set nn_gpu = gpu_tasks|calc_num_nodes(4) if 'gpu' in partition else 0 %}
    {% set nn = nn|default((nn_cpu, nn_gpu)|max, true) %}
    {% if partition == 'gpu' %}
#SBATCH --nodes={{ nn|default(1, true) }}
    {# Check to make sure requested tasks is a multiple of number of nodes. #}
    {% if (gpu_tasks, cpu_tasks)|max % nn == 0 %}
#SBATCH --ntasks-per-node={{ ((gpu_tasks, cpu_tasks)|max / nn)|int }}
    {% else %}
#SBATCH --ntasks={{ (gpu_tasks, cpu_tasks)|max }}
    {% endif %}
#SBATCH --gres=gpu:{{ gpu_tasks }}
    {% else %}{# def partition #}
#SBATCH --nodes={{ nn }}
    {# Check to make sure requested tasks is a multiple of number of nodes. #}
    {% if cpu_tasks % nn == 0 %}
#SBATCH --ntasks-per-node={{ (cpu_tasks / nn)|int }}
    {% else %}
#SBATCH --ntasks={{ cpu_tasks }}
    {% endif %}
    {% endif %}
{% endblock tasks %}
{% block header %}
    {{- super () -}}
    {% set account = account|default(project|get_account_name, true) %}
    {% if account %}
#SBATCH --account={{ account }}
    {% endif %}
{% endblock header %}
