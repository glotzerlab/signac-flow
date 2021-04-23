{# Templated in accordance with: https://www.psc.edu/resources/bridges-2/user-guide #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% set threshold = 0 if force else 0.9 %}
    {% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
    {% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
    {% if gpu_tasks %}
        {% if not ('GPU' in partition or force) %}
            {% raise "GPU operations require a GPU partition!" %}
        {% endif %}
        {#- GPU nodes have 8 NVIDIA V100-32GB SXM2 #}
        {% set nn_gpu = gpu_tasks|calc_num_nodes(8) %}
        {% set nn = nn_gpu %}
        {% else %}
        {% if 'GPU' in partition and not force %}
            {% raise "Requesting GPU partition, but no GPUs requested!" %}
        {% endif %}
        {% set nn = nn|default(cpu_tasks|calc_num_nodes(128), true) %}
    {% endif %}
    {% if 'GPU' in partition %}
        {% set gpus_per_node = (gpu_tasks / nn)|round(0, 'ceil')|int %}
        {% set cpus_per_node = (cpu_tasks / nn)|round(0, 'ceil')|int %}
        {% if cpus_per_node > gpus_per_node * 5 and not force %}
            {% raise "Cannot request more than 5 CPUs per GPU." %}
        {% endif %}
    {% endif %}
    {% if partition == 'GPU' %}
#SBATCH -N {{ nn|check_utilization(gpu_tasks, 8, threshold, 'GPU') }}
#SBATCH --ntasks-per-node={{ cpus_per_node }}
#SBATCH --gpus={{ gpu_tasks }}
    {% elif partition == 'GPU-shared' %}
#SBATCH -N {{ nn|check_utilization(gpu_tasks, 1, threshold, 'GPU') }}
#SBATCH --ntasks-per-node={{ cpus_per_node }}
#SBATCH --gpus={{ gpu_tasks }}
    {% elif partition == 'EM' %}
#SBATCH -N {{ nn|check_utilization(cpu_tasks, 96, threshold, 'CPU') }}
#SBATCH --ntasks-per-node={{ (96, cpu_tasks)|min }}
    {% elif partition == 'RM-shared' %}
        {% if nn|default(1, true) > 1 or cpu_tasks > 64 %}
            {% raise "Cannot request RM-shared with more than 64 tasks or multiple nodes." %}
        {% endif %}
#SBATCH -N {{ nn|default(1, true) }}
#SBATCH --ntasks={{ cpu_tasks }}
    {% else %}
{#- This should cover RM, RM-512, and possibly RM-small (not documented) #}
#SBATCH -N {{ nn|check_utilization(cpu_tasks, 128, threshold, 'CPU') }}
#SBATCH --ntasks-per-node={{ (128, cpu_tasks)|min }}
    {% endif %}
{% endblock tasks %}
{% block header %}
    {{- super() -}}
    {% set account = account|default(environment|get_account_name, true) %}
    {% if account %}
#SBATCH -A {{ account }}
    {% endif %}
{% endblock header %}
