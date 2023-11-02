{# Templated in accordance with: https://www.psc.edu/resources/bridges-2/user-guide #}
{% extends "slurm.sh" %}
{% block tasks %}
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
