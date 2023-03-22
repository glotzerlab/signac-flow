{# Templated in accordance with: https://www.psc.edu/resources/bridges-2/user-guide #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% if resources.gpu_tasks %}
        {% if not ('GPU' in partition or force) %}
            {% raise "GPU operations require a GPU partition!" %}
        {% endif %}
    {% else %}
        {% if 'GPU' in partition and not force %}
            {% raise "Requesting GPU partition, but no GPUs requested!" %}
        {% endif %}
    {% endif %}
    {% if partition == 'RM-shared' and resources.cpu_tasks > 64 %}
        {% raise "Cannot request RM-shared with more than 64 tasks or multiple nodes." %}
    {% endif %}
#SBATCH -N {{ resources.cpu_tasks }}
#SBATCH --ntasks={{ resources.cpu_tasks }}
    {% if 'GPU' in partition %}
#SBATCH --gpus={{ resources.gpu_tasks }}
    {% endif %}
{% endblock tasks %}
{% block header %}
    {{- super() -}}
    {% set account = account|default(project|get_account_name, true) %}
    {% if account %}
#SBATCH -A {{ account }}
    {% endif %}
{% endblock header %}
