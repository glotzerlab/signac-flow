{% extends "slurm.sh" %}
{# Must override this block before header block is created #}
{% block tasks %}
{% if partition == 'shared' %}
{% if num_tasks > 24 %}
{% raise "You cannot use more than 24 cores on the 'shared' partitions." %}
{% else %}
#SBATCH --nodes={{ 1 }}
#SBATCH --ntasks-per-node={{ num_tasks }}
{% endif %}
{% else %}
{% set nn = (num_tasks/24)|round(method='ceil')|int %}
{% set ntasks = 24 if num_tasks > 24 else num_tasks %}
#SBATCH --nodes={{ nn }}
#SBATCH --ntasks={{ ntasks }}
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
