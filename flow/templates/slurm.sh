{% extends "base_script.sh" %}
{% block header %}
#!/bin/bash
#SBATCH --job-name="{{ id }}"
{% if partition %}
#SBATCH --partition={{ partition }}
{% endif %}
{% if memory %}
#SBATCH --mem={{ memory }}
{% endif %}
{% if walltime %}
#SBATCH -t {{ walltime|format_timedelta }}
{% endif %}
{% if job_output %}
#SBATCH --output={{ job_output }}
#SBATCH --error={{ job_output }}
{% endif %}
{% block tasks %}
#SBATCH --ntasks={{ operations|calc_tasks('np', parallel, force) }}
{% endblock %}
{% endblock %}
