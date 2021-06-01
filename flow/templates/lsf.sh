{% extends "base_script.sh" %}
{% block header %}
#!/bin/bash
#BSUB -J {{ id }}
    {% set memory_requested = operations | calc_memory(parallel) %}
    {% if memory_requested %}
#BSUB -M {{ memory_requested|format_memory }}B
    {% endif %}
    {% if partition %}
#BSUB -q {{ partition }}
    {% endif %}
    {% set walltime = operations | calc_walltime(parallel) %}
    {% if walltime %}
#BSUB -W {{ walltime|format_timedelta(style='HH:MM') }}
    {% endif %}
    {% if job_output %}
#BSUB -eo {{ job_output }}
    {% endif %}
    {% block tasks %}
#BSUB -n {{ operations|calc_tasks('np', parallel, force) }}
    {% endblock tasks %}
{% endblock header %}
