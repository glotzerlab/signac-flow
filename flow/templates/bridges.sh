{% extends "slurm.sh" %}
{% block tasks %}
{% set cpn = 28 %}
{% if 'shared' in partition %}
{% set nn = nn|default(1, true) %}
{% set tpn = num_tasks %}
{% if tpn > cpn %}
{% raise "Cannot put more than %d tasks into one submission in 'shared' partition."|format(cpn) %}
{% endif %}
{% else %}
{% set nn = nn|default((num_tasks/cpn)|round(method='ceil')|int, true) %}
{% set node_util = num_tasks / (cpn * nn) %}
{% if not force and node_util < 0.9 %}
{% raise "Bad node utilization!! nn=%d, cores_per_node=%d, num_tasks=%d"|format(nn, cpn, num_tasks) %}
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
