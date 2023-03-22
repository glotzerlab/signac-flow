{% extends "slurm.sh" %}
{% set partition = partition|default('standard', true) %}
{% block tasks %}
    {% if resources.gpu_tasks and 'gpu' not in partition and not force %}
        {% raise "Requesting GPUs requires a gpu partition!" %}
    {% endif %}
#SBATCH --nodes={{ resources.num_nodes }}
    {% if partition == 'gpu' %}
#SBATCH --ntasks={{ (resources.ngpu_tasks, resources.ncpu_tasks)|max }}
#SBATCH --gpus={{ resources.ngpu_tasks }}
    {% else %}{# standard compute partition #}
#SBATCH --ntasks={{ resources.ncpu_tasks }}
    {% endif %}
{% endblock tasks %}
{% block header %}
    {{- super () -}}
    {% set account = account|default(project|get_account_name, true) %}
    {% if account %}
#SBATCH --account={{ account }}
    {% endif %}
{% endblock header %}
