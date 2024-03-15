{# Templated in accordance with: https://docs.olcf.ornl.gov/systems/crusher_quick_start_guide.html #}
{% extends "slurm.sh" %}
{% block tasks %}
#SBATCH --nodes={{ resources.num_nodes }}
{% endblock tasks %}
{% block header %}
    {{- super() -}}
#SBATCH --partition=batch
{% endblock header %}
