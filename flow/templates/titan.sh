{% extends "slurm.sh" %}
{% block header %}
{{ super() }}
{% set account = 'account'|get_config_value(ns=environment) %}
{% if account is not none %}
#SBATCH -A {{ account }}
{% endif %}
{% endblock %}
