{# Templated in accordance with: https://docs.olcf.ornl.gov/systems/andes_user_guide.html #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% set threshold = 0 if force else 0.9 %}
    {% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
    {% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
    {% if gpu_tasks %}
        {% if not ('GPU' in partition or force) %}
            {% raise "GPU operations require a GPU partition!" %}
        {% endif %}
        {# GPU nodes have 2 NVIDIA K80s #}
        {% set nn_gpu = gpu_tasks|calc_num_nodes(2) %}
        {% set nn = nn_gpu %}
    {% else %}
        {% if 'gpu' in partition and not force %}
            {% raise "Requesting gpu partition, but no GPUs requested!" %}
        {% endif %}
        {% set nn = nn|default(cpu_tasks|calc_num_nodes(32), true) %}
    {% endif %}
    {% if 'gpu' in partition %}
        {% set gpus_per_node = (gpu_tasks / nn)|round(0, 'ceil')|int %}
        {% set cpus_per_node = (cpu_tasks / nn)|round(0, 'ceil')|int %}
        {% if cpus_per_node > gpus_per_node * 14 and not force %}
            {% raise "Cannot request more than 14 CPUs per GPU." %}
        {% endif %}
    {% endif %}
    {% if partition == 'gpu' %}
#SBATCH -N {{ nn|check_utilization(gpu_tasks, 2, threshold, 'GPU') }}
#SBATCH --ntasks-per-node={{ cpus_per_node }}
#SBATCH --gpus={{ gpu_tasks }}
    {% else %}
        {# This should cover batch #}
#SBATCH -N {{ nn|check_utilization(cpu_tasks, 32, threshold, 'CPU') }}
#SBATCH --ntasks-per-node={{ (32, cpu_tasks)|min }}
    {% endif %}
{% endblock tasks %}
{% block header %}
    {{- super() -}}
    {% set account = account|default(environment|get_account_name, true) %}
    {% if account %}
#SBATCH -A {{ account }}
    {% endif %}
{% endblock header %}
