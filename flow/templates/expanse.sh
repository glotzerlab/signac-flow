{# Templated in accordance with: https://www.sdsc.edu/support/user_guides/expanse.html #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% if resources.gpu_tasks %}
        {% if not ('gpu' in partition or force) %}
            {% raise "GPU operations require a GPU partition!" %}
        {% endif %}
    {% else %}
        {% if 'gpu' in partition and not force %}
            {% raise "Requesting GPU partition, but no GPUs requested!" %}
        {% endif %}
    {% endif %}
    {% if "shared" in partition and resources.num_nodes > 1 %}
        {% raise "Cannot request shared partition with resources spanning multiple nodes." %}
    {% endif %}
    {% if "shared" not in partition %}
#SBATCH -N {{ resources.num_nodes }}
    {% endif %}
#SBATCH --ntasks={{ resources.ncpus_tasks }}
    {% if 'gpu' in partition %}
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
