{# Templated in accordance with: https://https://wiki.ncsa.illinois.edu/display/DSC/Delta+User+Guide #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% set threshold = 0 if force else 0.9 %}
    {% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
    {% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
    {% if gpu_tasks %}
        {% if partition == "gpuA40x4" %}
            {% set nn_gpu = gpu_tasks|calc_num_nodes(4) %}
            {% if nn_gpu > 100 %}
                {% raise "More nodes requested than exist on the system." %}
            {% endif %}
        {% elif partition == "gpuA100x4" %}
            {% set nn_gpu = gpu_tasks|calc_num_nodes(4) %}
            {% if nn_gpu > 100 %}
                {% raise "More nodes requested than exist on the system." %}
            {% endif %}
        {# We do not allow submission to the partitions below as they have few #}
        {# nodes, should be rarely needed, and have increased charges for use. #}
        {% elif partition == "gpuA100x8" %}
            {% raise "Submitting to gpuA100x8 partition is not allowed." %}
        {% elif partition == "gpuMI100x8" %}
            {% raise "Submitting to gpuMI100x8 partition is not allowed." %}
        {% else %}
            {% raise "GPU operations require a GPU partition!" %}
        {% endif %}
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
        {% if cpus_per_node > gpus_per_node * 16 and not force %}
            {% raise "Cannot request more than 16 CPUs per GPU." %}
        {% endif %}
    {% endif %}
    {% if "gpu" in partition %}
        {% if nn ==1 %}
#SBATCH -N {{ nn }}
        {% else %}
#SBATCH -N {{ nn|check_utilization(gpu_tasks, 4, threshold, 'GPU') }}
        {% endif %}
#SBATCH --ntasks-per-node={{ cpus_per_node }}
#SBATCH --gpus-per-node={{ gpus_per_node }}
    {% else %}
        {% if nn == 1 %}
#SBATCH -N {{ nn }}
#SBATCH --ntasks-per-node={{ (128, cpu_tasks)|min }}
        {% else %}
#SBATCH -N {{ nn|check_utilization(cpu_tasks, 128, threshold, 'CPU') }}
#SBATCH --ntasks-per-node={{ (128, cpu_tasks)|min }}
        {% endif %}
    {% endif %}
{% endblock tasks %}
{% block header %}
    {{- super() -}}
    {% set account = account|default(environment|get_account_name, true) %}
    {% if account %}
#SBATCH -A {{ account }}
    {% endif %}
{% endblock header %}
