{# Templated in accordance with: https://www.rcac.purdue.edu/knowledge/anvil/ #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% if resources.ngpu_tasks and not ("gpu" in partition or force) %}
        {% raise "GPU operations require a gpu partition!" %}
    {% endif %}
    {% if resources.ngpu_tasks == 0 %}
        {% if 'gpu' in partition and not force %}
            {% raise "Requesting gpu partition, but no GPUs requested!" %}
        {% endif %}
    {% endif %}
    {% if resources.num_nodes > 1 %}
#SBATCH -N {{ resources.num_nodes }}
    {% endif %}
#SBATCH --ntasks={{ resources.ncpu_tasks }}
    {% if 'gpu' in partition %}
#SBATCH --gpus={{ resources.ngpu_tasks }}
    {% endif %}
{% endblock tasks %}
{% block header %}
    {{- super() -}}
    {% set account = account|default(project|get_account_name, true) %}
    {% if account %}
#SBATCH --account {{ account }}
    {% endif %}
# As of 2023-10-30, Anvil incorrectly binds ranks to cores with `mpirun -n`.
# Disable core binding to work around this issue.
export OMPI_MCA_hwloc_base_binding_policy=""
{% endblock header %}