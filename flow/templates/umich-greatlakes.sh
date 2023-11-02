{% extends "slurm.sh" %}
{% set partition = partition|default('standard', true) %}
{% block tasks %}
#SBATCH --nodes={{ resources.num_nodes }}
#SBATCH --ntasks={{ resources.ncpu_tasks }}
    {% if partition == 'gpu' %}
#SBATCH --gpus={{ resources.ngpu_tasks }}
    {% endif %}
{% endblock tasks %}
{% block header %}
    {{- super () -}}
    {% set account = account|default(project|get_account_name, true) %}
    {% if account %}
#SBATCH --account={{ account }}
    {% endif %}
{% endblock header %}
