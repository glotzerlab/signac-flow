{% extends "torque.sh" %}
{% block header %}
{{ super() }}
{% set account = 'account'|get_config_value(ns=environment) %}
{% if account is not none %}
#PBS -A {{ account }}
{% endif %}
{% endblock %}
