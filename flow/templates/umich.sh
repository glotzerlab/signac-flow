{% extends "base_script.sh" %}
{% block header %}
#PBS -N {{ id }}
{% if walltime is not none %}
#PBS -l walltime={{ walltime|format_timedelta }}
{% endif %}
{% if nn is not none %}
{% if ppn is none %}
{% if mode == 'gpu' %}
#PBS -l nodes={{ nn }}:gpus=1
{% else %}
#PBS -l nodes={{ nn }}
{% endif %}
{% else %}
{% if mode == 'gpu' %}
#PBS -l nodes={{ nn }}:ppn={{ ppn }}:gpus=1
{% else %}
#PBS -l nodes={{ nn }}:ppn={{ ppn }}
{% endif %}
{% endif %}
{% endif %}
#PBS -l pmem={{ memory }}
{% if not no_copy_env %}
#PBS -V
{% endif %}
{% set account = 'account'|get_config_value(ns=environment,default=none) %}
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
