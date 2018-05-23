{# Templated in accordance with: http://www.sdsc.edu/support/user_guides/comet.html#running #}
{# Last updated on: 2018-05-22 #}
{% set mpiexec = "ibrun" %}
{% extends "slurm.sh" %}
{# Must override this block before header block is created #}
{% block tasks %}
{% set cpn = 24 %}
{% set gpn = 4 %}
{% set gpus = operations|map(attribute='directives.ngpu')|sum %}
{% if gpus %}
{% set ngpus = gpus if parallel else operations|map(attribute='directives.ngpu')|max %}
{% if partition == 'gpu' %}
{% set nn = nn|default((np_global/cpn)|round(method='ceil')|int, true) %}
{% set nn_gpu = (ngpus/gpn)|round(method='ceil')|int %}
{% set nn = nn if nn > nn_gpu else nn_gpu %}
{% set node_util = ngpus / (gpn * nn) %}
{% if not force and node_util < 0.9 %}
{% raise "Bad node utilization!! nn=%d, gpus_per_node=%d, ngpus=%d"|format(nn, gpn, ngpus) %}
{% endif %}
{% set ngpupn = gpn if ngpus > gpn else ngpus %}
#SBATCH --nodes={{ nn }}
#SBATCH --gres=gpu:p100:{{ ngpupn }}
{% elif partition == 'gpu-shared' %}
#SBATCH --nodes={{ nn|default(1, true) }}
#SBATCH --ntasks-per-node={{ 7 * ngpus }}
#SBATCH --gres=gpu:p100:{{ ngpus }}
{% else %}
{% raise "Submitting operations with the gpu directive requires a GPU partition." %}
{% endif %}
{% else %}
{% if 'shared' in partition %}
#SBATCH --nodes={{ nn|default(1, true) }}
#SBATCH --ntasks-per-node={{ np_global }}
{% else %}
{% set nn = nn|default((np_global/cpn)|round(method='ceil')|int, true) %}
{% set node_util = np_global / (cpn * nn) %}
{% if not force and node_util < 0.9 %}
{% raise "Bad node utilization!! nn=%d, cores_per_node=%d, np_global=%d"|format(nn, cpn, np_global) %}
{% endif %}
{% set tpn = cpn if np_global > cpn else np_global %}
#SBATCH --nodes={{ nn }}
#SBATCH --ntasks-per-node={{ tpn }}
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
