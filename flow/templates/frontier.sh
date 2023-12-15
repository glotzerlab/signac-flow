{# Templated in accordance with: https://docs.olcf.ornl.gov/systems/crusher_quick_start_guide.html #}
{% extends "slurm.sh" %}
{% block tasks %}
#SBATCH --nodes={{ resources.num_nodes }}
{% endblock tasks %}
{% block header %}
    {{- super() -}}
    {% set account = account|default(project|get_account_name, true) %}
    {% if account %}
#SBATCH --account={{ account }}
    {% endif %}
{% endblock header %}
