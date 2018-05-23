{% extends "torque.sh" %}
{# Must come before header is written #}
{% block tasks %}
{% set s_gpu = ':gpus=1' if mode == 'gpu' else '' %}
{% set s_ppn = ':ppn=' ~ ppn if ppn else '' %}
{% set nn = nn|default((np_global/ppn)|round(method='ceil')|int if ppn else np_global, true) %}
#PBS -l nodes={{ nn }}{{ s_ppn }}{{ s_gpu }}
{% endblock %}

{% block header %}
{{ super() -}}
#PBS -l pmem={{ memory }}
{% set account = 'account'|get_config_value(ns=environment) %}
{% if account %}
#PBS -A {{ account }}
{% endif %}
{% set qos = 'qos'|get_config_value(environment, 'flux') %}
#PBS -l qos={{ qos }}
{% set queue = 'cpu_queue'|get_config_value(environment, 'flux') %}
{% if mode == 'gpu' %}
{% set queue = queue+'g' %}
{% endif %}
#PBS -q {{ queue }}
{% endblock %}
