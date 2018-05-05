{% extends "base_submit.sh" %}
{% block header %}
#PBS -N {{ id }}
{% if walltime is not none %}
#PBS -l walltime={{ walltime|timedelta }}
{% endif %}
{% if nn is not none %}
{% if ppn is none %}
#PBS -l nodes={{ nn }}
{% else %}
#PBS -L nodes={{ nn }}:ppn={{ ppn }}
{% endif %}
{% endif %}
{% if not no_copy_env %}
#PBS -V
{% endif %}
{% endblock %}
