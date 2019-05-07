{# The following variables are available to all scripts. #}
{% if detailed %}
{% set alias = scheduler_status_code %}
{% set format_progress_bar = ("%%-%ss  %%-%ss" | format(label_length,bar_length,)) %}
{% set format_job_id = ("%%-%ss  " | format(id_length,)) %}
{% set format_operation = ("%%-%ss %%3s  " | format(operation_length,)) %}
{% set format_operation_title = ("%%-%ss  " | format(operation_length+4,)) %}
{% set format_label = ("%%-%ss" | format(total_label_length,)) %}

{% set ns = namespace(dash='', format_parameters='') %}
{% if parameters %}
{% for para in parameters %}
{% set ns.dash = ns.dash + ("%s  " | format('-' * parameters_length[loop.index-1])) %}
{% set ns.format_parameters = ns.format_parameters + ("%%-%ss  " | format(parameters_length[loop.index-1],)) %}
{% endfor %}
{% set para_head = ns.format_parameters | format(*parameters,) %}
{% endif %}
{% set format_head = format_job_id + format_operation_title + '%s' + format_label %}
{% set format_row = format_job_id + format_operation + '%s' + format_label %}
{% endif %}
{% block body %}
{% if overview %}
# Overview:
Total # of jobs: {{'%s' |format(jobs_count)}}

{{ format_progress_bar | format('label','ratio')}}
{{ format_progress_bar | format('-'*label_length,'-'*bar_length)}}
{% for label in progress_sorted %}
{{ format_progress_bar | format(label[0], label[1] | draw_progressbar(jobs_count))}}
{% endfor %}
{% endif %}

{% if detailed %}
# Detailed View:
{{ format_head | format('job_id', 'opeartions', para_head, 'labels',) }}
{{ format_head | format('-'*id_length, '-'*(operation_length+4), ns.dash, '-'*total_label_length,) }}

{% for job in jobs %}
{% if parameters %}
{% set para_output = ns.format_parameters | format(*job['parameters'].values()) %}
{% endif %}
{% set ns.first_row = True %}
{% for key,value in job['operations'].items() %}
{% if (alias[value['scheduler_status']] != 'U' or value['eligible']) or all_ops%}
{% if ns.first_row %}
{{ format_job_id | format(job['job_id'],) }}{{ format_operation | bold_font(value['eligible'],pretty) | format(key,'['+alias[value['scheduler_status']]+']',) }}{{para_output}}{{ '%s' | format(job['labels']|join(', ')) }}
{% set ns.first_row = false %}
{% else %}
{{format_job_id | format('',)}}{{ format_operation | bold_font(value['eligible'],pretty) | format(key,'['+alias[value['scheduler_status']]+']',) }}{{para_output}}{{ '%s' | format(job['labels']|join(', ')) }}
{% endif %}
{% endif %}
{% endfor %}
{% endfor %}
{% endif %}
[U]:unknown [R]:registered [Q]:queued [A]:active [I]:inactive [!]:requires_attention

{% endblock %}
