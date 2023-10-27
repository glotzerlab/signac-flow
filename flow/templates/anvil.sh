{# Templated in accordance with: https://www.rcac.purdue.edu/knowledge/anvil/ #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% if resources.ngpu_tasks %}
        {% if not ('gpu' in partition or force) %}
            {% raise "GPU operations require a gpu partition!" %}
        {% endif %}
        {% if resources.ngpu_tasks > 4 %}
            {% raise "Cannot request a gpu job with more than 4 GPUs." %}
        {% endif %}
    {% else %}
        {% if 'gpu' in partition and not force %}
            {% raise "Requesting gpu partition, but no GPUs requested!" %}
        {% endif %}
    {% endif %}
    {% if partition in ['debug', 'shared', 'highmem'] and resources.ncpu_tasks > 128 %}
        {% raise "Cannot request a shared partition with more than 128 CPU tasks." %}
    {% endif %}
    {% if resources.num_nodes > 1 or resources.ncpu_tasks >= 128 or resources.ngpu_tasks >= 4 %}
#SBATCH -N {{ resources.num_nodes }}
    {% endif %}
#SBATCH --ntasks={{ resources.ncpu_tasks }}
    {% if 'gpu' in partition %}
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
