{# Templated in accordance with: https://www.psc.edu/resources/bridges-2/user-guide #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% if resources.ngpu_tasks %}
        {% if not ('GPU' in partition or force) %}
            {% raise "GPU operations require a GPU partition!" %}
        {% endif %}
        {% if partition == "GPU-shared" and resources.ngpu_tasks > 4 %}
            {% raise "Cannot request GPU-shared with more than 4 GPUs." %}
        {% endif %}
    {% else %}
        {% if 'GPU' in partition and not force %}
            {% raise "Requesting GPU partition, but no GPUs requested!" %}
        {% endif %}
    {% endif %}
    {% if partition == 'RM-shared' and resources.ncpu_tasks > 64 %}
        {% raise "Cannot request RM-shared with more than 64 tasks or multiple nodes." %}
    {% endif %}
    {% if resources.num_nodes > 1 or resources.ncpu_tasks >= 128 or resources.ngpu_tasks >= 8 %}
#SBATCH -N {{ resources.num_nodes }}
    {% endif %}
#SBATCH --ntasks={{ resources.ncpu_tasks }}
    {% if 'GPU' in partition %}
#SBATCH --gpus={{ resources.ngpu_tasks }}
    {% endif %}
{% endblock tasks %}
{% block header %}
    {{- super() -}}
    {% set account = account|default(project|get_account_name, true) %}
    {% if account %}
#SBATCH -A {{ account }}
    {% endif %}
{% endblock header %}
