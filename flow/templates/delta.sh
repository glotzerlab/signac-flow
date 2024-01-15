{# Templated in accordance with: https://https://wiki.ncsa.illinois.edu/display/DSC/Delta+User+Guide #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% if partition in ["gpuA100x8", "gpuMI100x8"] %}
        {% raise "This partition is not supported as it has few nodes,
                  increased charges and is expected to be suitable for a
                  minority of use cases." %}
    {% endif %}
    {% if resources.num_nodes > 1 %}
#SBATCH -N {{ resources.num_nodes }}
    {% endif %}
#SBATCH --ntasks={{ resources.ncpu_tasks }}
    {% if "gpu" in partition %}
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
