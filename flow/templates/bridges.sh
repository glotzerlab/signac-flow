{% set mpiexec = "mpirun" %}
{% extends "slurm.sh" %}
{% block tasks %}
{% set cpn = 28 %}
{% if 'shared' in partition %}
{% set nn = nn|default(1, true) %}
{% set tpn = np_global %}
{% if tpn > cpn %}
{% raise "Cannot put more than %d tasks into one submission in 'shared' partition."|format(cpn) %}
{% endif %}
{% else %}
{% set nn = nn|default((np_global/cpn)|round(method='ceil')|int, true) %}
{% set node_util = np_global / (cpn * nn) %}
{% if not force and node_util < 0.9 %}
{% raise "Bad node utilization!! nn=%d, cores_per_node=%d, np_global=%d"|format(nn, cpn, np_global) %}
{% endif %}
{% set tpn = cpn if np_global > cpn else np_global %}
{% endif %}
#SBATCH -N {{ nn }}
#SBATCH --ntasks-per-node {{ tpn }}
{% endblock %}
{% block gpu %}
{% set gpus = operations|map(attribute='directives.ngpu')|sum %}
{% if gpus %}
{% if operations|map(attribute='directives.ngpu')|identical %}
{% set ngpu = operations[0].directives.ngpu %}
{% else %}
{% raise "The gpu directive must be identical for all operations." %}
{% endif %}
{% if partition == 'GPU' %}
{% if ngpu > 2 %}
{% raise "Max. of two P100 GPUs per node." %}
{% endif %}
#SBATCH --gres=gpu:p100:{{ ngpu }}
{% elif partition == 'GPU-shared' %}
{% if ngpu > 1 %}{% raise "The gpu directive must be 1 for the shared partition." %}{% endif %}
#SBATCH --gres=gpu:p100:1
{% else %}
{% raise "Submitting operations with the gpu directive requires a GPU partition." %}
{% endif %}
{% endif %}
{% endblock %}

{% block header %}
{{ super () -}}
{% set account = 'account'|get_config_value(ns=environment) %}
{% if account %}
#SBATCH -A {{ account }}
{% endif %}
{% if memory %}
#SBATCH --mem={{ memory }}G
{% endif %}
{% endblock %}
