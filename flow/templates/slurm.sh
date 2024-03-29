{% extends "base_script.sh" %}
{% block header %}
    {% block preamble %}
#!/bin/bash
#SBATCH --job-name="{{ id }}"
        {% set memory_requested = operations | calc_memory(parallel)  %}
{% block memory scoped %}
        {% if memory_requested %}
#SBATCH --mem={{ memory_requested|format_memory }}
        {% endif %}
{% endblock memory %}
        {% if partition %}
#SBATCH --partition={{ partition }}
        {% endif %}
        {% set walltime = operations | calc_walltime(parallel) %}
        {% if walltime %}
#SBATCH -t {{ walltime|format_timedelta }}
        {% endif %}
        {% if job_output %}
#SBATCH --output={{ job_output }}
#SBATCH --error={{ job_output }}
        {% endif %}
    {% endblock preamble %}
    {% block tasks %}
#SBATCH --ntasks={{ resources.ncpu_tasks }}
    {% endblock tasks %}
{% endblock header %}
