{% extends "base_submit.sh" %}
{% block header %}
#!/bin/bash
#SBATCH --job-name="{{ id }}"
{% if nn is not none %}
#SBATCH --nodes={{ nn }}
{% endif %}
{% if ppn is not none %}
#SBATCH --ntasks-per-node={{ ppn }}
{% endif %}
{% if walltime is not none %}
#SBATCH -t {{ walltime|timedelta }}
{% endif %}
{% endblock %}
