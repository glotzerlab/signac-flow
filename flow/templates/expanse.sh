{# Templated in accordance with: https://www.sdsc.edu/support/user_guides/expanse.html #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% if "shared" not in partition %}
#SBATCH -N {{ resources.num_nodes }}
    {% endif %}
#SBATCH --ntasks={{ resources.ncpus_tasks }}
    {% if 'gpu' in partition %}
#SBATCH --gpus={{ resources.gpu_tasks }}
    {% endif %}
{% endblock tasks %}
{% block header %}
    {{- super() -}}
    {% set account = account|default(project|get_account_name, true) %}
    {% if account %}
#SBATCH -A {{ account }}
    {% endif %}
{% endblock header %}
