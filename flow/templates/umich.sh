{% extends "base_script.sh" %}
{% block header %}
#PBS -N {{ id }}
{% if walltime is not none %}
#PBS -l walltime={{ walltime|format_timedelta }}
{% endif %}
{% set s_gpu = ':gpus=1' if mode == 'gpu' else ''  %}
{% set s_ppn = ':ppn=' ~ ppn if ppn else ''  %}
#PBS -l nodes={{ nn }}{{ s_ppn }}{{ s_gpu }}
#PBS -l pmem={{ memory }}
{% if not no_copy_env %}
#PBS -V
{% endif %}
{% set account = 'account'|get_config_value(ns=environment) %}
{% if account is not none %}
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
