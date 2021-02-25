{% extends "base_script.sh" %}
{% block header %}
#!/bin/bash
#BSUB -J {{ id }}
{% set memory_passed = operations | check_memory(memory) %}
{% if memory_passed %}
#BSUB -M {{ operations | calc_memory(memory) }}GB
{% endif %}
{% if partition %}
#BSUB -q {{ partition }}
{% endif %}
{% if walltime %}
#BSUB -W {{ walltime|format_timedelta(style='HH:MM') }}
{% endif %}
{% if job_output %}
#BSUB -eo {{ job_output }}
{% endif %}
{% block tasks %}
#BSUB -n {{ operations|calc_tasks('np', parallel, force) }}
{% endblock %}
{% endblock %}
