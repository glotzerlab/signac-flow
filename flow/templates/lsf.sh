{% extends "base_script.sh" %}
{% block header %}
#!/bin/bash
#BSUB -J {{ id }}
{% set memory_requested = operations | calc_memory(parallel) %}
{% if memory_requested %}
#BSUB -M {{ memory_requested }}GB
{% endif %}
{% if partition %}
#BSUB -q {{ partition }}
{% endif %}
{% set walltime = walltime | default(operations | calc_walltime(parallel) , True) %}
{% if walltime %}
#BSUB -W {{ walltime|format_timedelta(style='HH:MM') }}
{% else %}
#BSUB -W 12:00
{% endif %}
{% if job_output %}
#BSUB -eo {{ job_output }}
{% endif %}
{% block tasks %}
#BSUB -n {{ operations|calc_tasks('np', parallel, force) }}
{% endblock %}
{% endblock %}
