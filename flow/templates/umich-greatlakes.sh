{% extends "slurm.sh" %}
{% set partition = partition|default('standard', true) %}
{% block tasks %}
    {% if resources.ngpu_tasks and 'gpu' not in partition and not force %}
        {% raise "Requesting GPUs requires a gpu partition!" %}
    {% endif %}
    {% if 'gpu' in partition and resources.ngpu_tasks == 0 and not force %}
        {% raise "Requesting gpu partition without GPUs!" %}
    {% endif %}
#SBATCH --nodes={{ resources.num_nodes }}-{{ resources.num_nodes }}
#SBATCH --ntasks={{ resources.nranks }}
#SBATCH --cpus-per-task={{ resources.ncpu_tasks // resources.nranks}}
    {% if partition == 'gpu' %}
#SBATCH --gpus-per-task={{ resources.ngpu_tasks // resources.nranks }}
    {% endif %}
{% endblock tasks %}
{% block header %}
    {{- super () -}}
    {% set account = account|default(project|get_account_name, true) %}
    {% if account %}
#SBATCH --account={{ account }}
    {% endif %}
{% endblock header %}
