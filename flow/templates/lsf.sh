{% extends "base_script.sh" %}
{% block header %}
#!/bin/bash
#BSUB -J {{ id }}
#BSUB -M {{ operations | calc_memory(memory) }}G
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
