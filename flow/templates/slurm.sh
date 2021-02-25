{% extends "base_script.sh" %}
{% block header %}
{% block preamble %}
#!/bin/bash
#SBATCH --job-name="{{ id }}"
{% set memory_passed = operations | check_memory(memory) %}
{% if memory_passed %}
#SBATCH --mem={{ operations | calc_memory(memory) }}G
{% endif %}
{% if partition %}
#SBATCH --partition={{ partition }}
{% endif %}
{% if walltime %}
#SBATCH -t {{ walltime|format_timedelta }}
{% endif %}
{% if job_output %}
#SBATCH --output={{ job_output }}
#SBATCH --error={{ job_output }}
{% endif %}
{% endblock %}
{% block tasks %}
#SBATCH --ntasks={{ operations|calc_tasks('np', parallel, force) }}
{% endblock %}
{% endblock %}
