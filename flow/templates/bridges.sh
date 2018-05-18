{% extends "slurm.sh" %}
{% block tasks %}
{% if 'shared' in partition %}
{% set nn = 1 %}
{% set tpn = num_tasks %}
{% if tpn > 28 %}
{% raise "Cannot put more than 28 tasks into one submission in 'shared' partition." %}
{% endif %}
{% else %}
{% set nn = (num_tasks/28)|round(method='ceil')|int %}
{% set node_util = num_tasks / (28 * nn) %}
{% if not force and node_util < 0.9 %}
{% raise "Bad node utilization!!" %}
{% endif %}
{% endif %}
{% if partition == 'GPU-shared' %}
{% if tpn > 2 %}
{% raise "Max. of two tasks per submission for GPU-shared partition." %}
{% endif %}
#SBATCH --gres=gpu:p100:{{ tpn }}
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
