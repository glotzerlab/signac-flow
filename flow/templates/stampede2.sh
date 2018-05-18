{% extends "slurm.sh" %}
{# Must override this block before header block is created #}
{% block tasks %}
{% if operations|map(attribute='directives.tpn')|identical %}
{# Easy way to get the value #}
{% set tpn = operations|map(attribute='directives.tpn')|max %}
{% else %}
{% raise "Cannot submit operations requiring different tpn." %}
{% endif %}
{% set cpn = 48 if 'skx' in partition else 68 %}
{% set nn = (num_tasks/cpn)|round(method='ceil')|int %}
{% set node_util = num_tasks / (nn * cpn) %}
{% if not force and node_util < 0.9 %}
{% raise "Bad node utilization!!" %}
{% endif %}
#SBATCH --nodes={{ nn }}
#SBATCH --ntasks-per-node={{ tpn }}
{% endblock %}

{% block header %}
{{ super () -}}
{% set account = 'account'|get_config_value(ns=environment) %}
{% if account %}
#SBATCH -A {{ account }}
{% endif %}
#SBATCH --partition={{ partition }}
{% endblock %}
