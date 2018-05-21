{% extends "slurm.sh" %}
{# Must override this block before header block is created #}
{% block tasks %}
{% set cpn = 24 %}
{% if 'shared' in partition %}
{% if num_tasks > cpn %}
{% raise "You cannot use more than 24 cores on the 'shared' partitions." %}
{% else %}
#SBATCH --nodes={{ 1 }}
#SBATCH --ntasks-per-node={{ num_tasks }}
{% endif %}
{% else %}
{% set nn = nn|default((num_tasks/cpn)|round(method='ceil')|int, true) %}
{% set ntasks = cpn if num_tasks > cpn else num_tasks %}
{% set node_util = num_tasks / (cpn * nn) %}
{% if not force and node_util < 0.9 %}
{% raise "Bad node utilization!! nn=%d, cores_per_node=%d, num_tasks=%d"|format(nn, cpn, num_tasks) %}
{% endif %}
#SBATCH --nodes={{ nn }}
#SBATCH --ntasks-per-node={{ ntasks }}
{% endif %}
{% if partition == 'gpu-shared' %}
#SBATCH --gres=gpu:p100:{{ num_tasks }}
{% endif %}
{% endblock %}
{% block header %}
{{ super () -}}
{% set account = 'account'|get_config_value(ns=environment) %}
{% if account %}
#SBATCH -A {{ account }}
{% endif %}
{% if memory %}
#SBATCH --mem={{ memory }}G
{% endif %}
{% endblock %}
