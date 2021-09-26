{# Templated in accordance with: https://www.sdsc.edu/support/user_guides/expanse.html #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% set threshold = 0 if force else 0.9 %}
    {% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
    {% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
    {% if gpu_tasks %}
        {% if not ('gpu' in partition or force) %}
            {% raise "GPU operations require a GPU partition!" %}
        {% endif %}
        {# GPU nodes have 4 NVIDIA V100-32GB SMX2 #}
        {% set nn_gpu = gpu_tasks|calc_num_nodes(4) %}
        {% set nn = nn_gpu %}
    {% else %}
        {% if 'gpu' in partition and not force %}
            {% raise "Requesting GPU partition, but no GPUs requested!" %}
        {% endif %}
        {% set nn = nn|default(cpu_tasks|calc_num_nodes(128), true) %}
    {% endif %}
    {% if 'gpu' in partition %}
        {% set gpus_per_node = (gpu_tasks / nn)|round(0, 'ceil')|int %}
        {% set cpus_per_node = (cpu_tasks / nn)|round(0, 'ceil')|int %}
        {% if cpus_per_node > gpus_per_node * 10 and not force %}
            {% raise "Cannot request more than 10 CPUs per GPU." %}
        {% endif %}
    {% endif %}

    {% if partition == 'gpu' %}
#SBATCH -N {{ nn|check_utilization(gpu_tasks, 4, threshold, 'GPU') }}
#SBATCH --ntasks-per-node={{ cpus_per_node }}
#SBATCH --gpus={{ gpu_tasks }}
    {% elif partition == 'gpu-shared' %}
        {% if nn|default(1, true) > 1 %}
            {% raise "Cannot request shared partition with resources spanning multiple nodes." %}
        {% endif %}
#SBATCH -N {{ nn|check_utilization(gpu_tasks, 1, threshold, 'GPU') }}
#SBATCH --ntasks-per-node={{ cpus_per_node }}
#SBATCH --gpus={{ gpu_tasks }}
    {% elif partition == 'shared' %}
        {% if nn|default(1, true) > 1 %}
            {% raise "Cannot request shared partition with resources spanning multiple nodes." %}
        {% endif %}
#SBATCH -N 1
#SBATCH --ntasks={{ cpu_tasks }}
    {% else %}
{# This should cover compute and large-memory #}
#SBATCH -N {{ nn|check_utilization(cpu_tasks, 128, threshold, 'CPU') }}
#SBATCH --ntasks-per-node={{ (128, cpu_tasks)|min }}
    {% endif %}
{% endblock tasks %}
{% block header %}
    {{ super() -}}
    {% set account = account|default(environment|get_account_name, true) %}
    {% if account %}
#SBATCH -A {{ account }}
    {% endif %}
{% endblock header %}
