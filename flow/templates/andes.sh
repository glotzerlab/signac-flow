{# Templated in accordance with: https://docs.olcf.ornl.gov/systems/andes_user_guide.html #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% if resources.ngpu_tasks %}
        {% if not ('GPU' in partition or force) %}
            {% raise "GPU operations require a GPU partition!" %}
        {% endif %}
    {% else %}
        {% if 'gpu' in partition and not force %}
            {% raise "Requesting gpu partition, but no GPUs requested!" %}
        {% endif %}
    {% endif %}
    {% if 'gpu' in partition %}
        {% if resources.ncpu_tasks > resources.ngpu_tasks * 14 and not force %}
            {% raise "Cannot request more than 14 CPUs per GPU." %}
        {% endif %}
    {% endif %}
#SBATCH -N {{ resources.num_nodes }}
#SBATCH --ntasks={{ resources.ncpus_tasks }}
    {% if partition == 'gpu' %}
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
