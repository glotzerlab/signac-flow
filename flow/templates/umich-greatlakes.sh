{% extends "slurm.sh" %}
{% set partition = partition|default('standard', true) %}
{% set nranks = (operations|calc_tasks("nranks", parallel, force), 1) | max %}
{% block tasks %}
#SBATCH --nodes={{ resources.num_nodes }}-{{ resources.num_nodes }}
#SBATCH --ntasks={{ nranks }}
#SBATCH --cpus-per-task={{ resources.ncpu_tasks // nranks}}
    {% if partition == 'gpu' %}
#SBATCH --gpus-per-task={{ resources.ngpu_tasks // nranks }}
    {% endif %}
{% endblock tasks %}
{% block header %}
    {{- super () -}}
    {% set account = account|default(project|get_account_name, true) %}
    {% if account %}
#SBATCH --account={{ account }}
    {% endif %}
{% endblock header %}
