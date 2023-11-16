{% extends "base_script.sh" %}
{% block header %}
    {% block preamble %}
#!/bin/bash
#SBATCH --job-name="{{ id }}"
        {% if partition %}
#SBATCH --partition={{ partition }}
        {% endif %}
        {% if walltime != None %}
#SBATCH -t {{ resources.walltime|format_timedelta }}
        {% endif %}
        {% if job_output %}
#SBATCH --output={{ job_output }}
#SBATCH --error={{ job_output }}
        {% endif %}
    {% set account = account|default(project|get_account_name, true) %}
    {% if account %}
#SBATCH --account={{ account }}
    {% endif %}
    {% endblock preamble %}
    {% block tasks %}
#SBATCH --ntasks={{ resources.processes }}
#SBATCH --cpus-per-task={{ resources.threads_per_process }}
#SBATCH --mem-per-task={{ resources.memory_per_cpu }}
    {% if resources.gpus_per_process > 0 %}
#SBATCH --gpus-per-task={{ resources.gpus_per_process }}
    {% endif %}
    {% endblock tasks %}
{% endblock header %}
