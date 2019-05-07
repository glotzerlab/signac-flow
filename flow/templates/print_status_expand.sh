{% extends "print_status.sh" %}
{% block body %}
{{ super () }}
## Operations:
{% set operation_length = [operation_length,9] | max %}
{% set format_operation = ("%%-%ss  " | format(operation_length,)) %}
{% set format_op_table = format_job_id + format_operation + '%-8s  ' + '%-14s  ' %}
{{ format_op_table | format('job_id', 'operation', 'eligible', 'cluster_status',) }}
{{ format_op_table | format('-'*id_length, '-'*operation_length, '-'*8, '-'*14,) }}
{% for job in jobs %}
{% for key,value in job['operations'].items() %}
{% if (alias[value['scheduler_status']] != 'U' or value['eligible']) or all_ops%}
{{ format_job_id | format(job['job_id'],) }}{{ format_operation | bold_font(value['eligible'],pretty) | format(key,) }}{{ '%-8s  ' | format(alias_bool[value['eligible']],)}}{{ '%-14s  ' | format(alias[value['scheduler_status']],)}}
{% endif %}
{% endfor %}
{% endfor %}
{% endblock %}
