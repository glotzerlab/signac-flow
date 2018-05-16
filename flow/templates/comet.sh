{% extends "base_script.sh" %}
{% block header %}
#!/bin/bash
#SBATCH --job-name="{{ id }}"
{% set account = 'account'|get_config_value(ns=environment,default=none) %}
{% if account is not none %}
#SBATCH -A {{ account }}
{% endif %}
{% if memory is not none %}
#SBATCH --mem={{ memory }}G
{% endif %}
{% if job_output is not none %}
#SBATCH --output={{ job_output }}
#SBATCH --error={{ job_output }}
{% endif %}
{% if walltime %}
#SBATCH -t {{ walltime|format_timedelta }}
{% endif %}
{% if parallel %}
{% set num_tasks = operations|map(attribute='directives.np')|sum %}
{% else %}
{% set num_tasks = operations|map(attribute='directives.np')|max %}
{% endif %}
#SBATCH --partition={{ partition }}
{% if partition == 'shared' %}
{% if num_tasks > 24 %}
{% raise "You cannot use more than 24 cores on the 'shared' partitions" %}
{% else %}
#SBATCH --nodes={{ 1 }}
#SBATCH --ntasks-per-node={{ num_tasks }}
{% endif %}
{% else %}
{% set nn = (num_tasks/24)|round(method='ceil')|int %}
{% set num_tasks = 24 if num_tasks > 24 else num_tasks %}
#SBATCH --nodes={{ nn }}
#SBATCH --ntasks={{ num_tasks }}
{% endif %}
{% endblock %}
