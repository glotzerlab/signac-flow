{% extends "base_script.sh" %}
{% block header %}
#!/bin/bash
#SBATCH --job-name="{{ id }}"
{% if parallel %}
{% set num_tasks = operations|map(attribute='directives.np')|sum %}
{% else %}
{% set num_tasks = operations|map(attribute='directives.np')|max %}
{% endif %}
#SBATCH --ntasks={{ num_tasks }}
{% if walltime %}
#SBATCH -t {{ walltime|format_timedelta }}
{% endif %}
{% block options %}
{% endblock %}
{% endblock %}
