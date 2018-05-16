{% extends "base_script.sh" %}
{% block header %}
#!/bin/bash
#SBATCH --job-name="{{ id }}"
{% set account = 'account'|get_config_value(ns=environment,default=none) %}
{% if account is not none %}
#SBATCH -A {{ account }}
{% endif %}
{% if job_output is not none %}
#SBATCH --output={{ job_output }}
#SBATCH --error={{ job_output }}
{% endif %}
{% if walltime %}
#SBATCH -t {{ walltime|format_timedelta }}
{% endif %}
{% if operations|map(attribute='directives.tpn')|identical %}
{# Easy way to get the value #}
{% set tpn = operations|map(attribute='directives.tpn')|max %}
{% else %}
{% raise "Cannot submit operations requiring different tpn." %}
{% endif %}
{% if parallel %}
{% set num_tasks = operations|map(attribute='directives.np')|sum %}
{% else %}
{% set num_tasks = operations|map(attribute='directives.np')|max %}
{% endif %}
#SBATCH --partition={{ partition }}
{% cpn = 48 if 'skx' in partition else 68 %}
{% set nn = (num_tasks/cpn)|round(method='ceil')|int %}
#SBATCH --nodes={{ nn }}
#SBATCH --ntasks-per-node={{ tpn }}
{% endblock %}
