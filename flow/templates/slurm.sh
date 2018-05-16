{% extends "base_script.sh" %}
{% block header %}
#!/bin/bash
#SBATCH --job-name="{{ id }}"
#SBATCH --ntasks={{ num_tasks }}
{% if walltime %}
#SBATCH -t {{ walltime|format_timedelta }}
{% endif %}
{% endblock %}
