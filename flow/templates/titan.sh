{% extends "torque.sh" %}
{# Must come before header is written #}
{% block tasks %}
{% set cpn = 16 %}
{% set nn = nn|default((np_global/cpn)|round(method='ceil')|int, true) %}
{% set node_util = np_global / (cpn * nn) %}
{% if not force and node_util < 0.9 %}
{% raise "Bad node utilization!! nn=%d, cores_per_node=%d, np_global=%d"|format(nn, cpn, np_global) %}
{% endif %}
#PBS -l nodes={{ nn }}
{% endblock %}
{% block header %}
{{ super() -}}
{% set account = 'account'|get_config_value(ns=environment) %}
{% if account %}
#PBS -A {{ account }}
{% endif %}
{% endblock %}
