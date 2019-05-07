{# The following variables are available to all scripts. #}
{% if detailed %}
{% set alias = scheduler_status_code %}
{% set format_progress_bar = ("%%-%ss  %%-%ss" | format(label_length,bar_length,)) %}
{% set format_job_id = ("%%-%ss " | format(id_length,)) %}
{% set format_operation = ("%%-%ss %%3s" | format(operation_length,)) %}
{% set format_operation_title = ("%%-%ss " | format(operation_length+4,)) %}
{% set format_label = ("%%-%ss" | format(total_label_length,)) %}
{% if parameters %}
{% set format_parameters = (("%%-%ss " * parameters|length()) | format(*parameters_length,)) %}
{% set format_head = format_job_id + format_operation_title + format_label + format_parameters %}
{% set format_row = format_job_id + format_operation + format_label + format_parameters %}
{% else %}
{% set format_head = format_job_id + format_operation_title + format_label %}
{% set format_row = format_job_id + format_operation + format_label %}
{% endif %}
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
{{ format_head | format('job_id', 'opeartions', 'labels', *parameters,)}}
{{ format_head | format('-'*id_length, '-'*(operation_length+4), '-'*total_label_length, *parameters,)}}

{% for job in jobs %}
{% set ns = namespace(first_row=true) %}
{% for key,value in job['operations'].items() %}
{% if (alias[value['scheduler_status']] != 'U' or value['eligible']) or all_ops%}
{% if ns.first_row %}
{{format_job_id | format(job['job_id'],)}} {{ format_operation | bold_font(value['eligible'],pretty) | format(key,'['+alias[value['scheduler_status']]+']',) }} {{ '%s' | format(job['labels']|join(', ')) }}
{% set ns.first_row = false %}
{% else %}
{{format_job_id | format('',)}} {{ format_operation | bold_font(value['eligible'],pretty) | format(key,'['+alias[value['scheduler_status']]+']',) }} {{ '%s' | format(job['labels']|join(', ')) }}
{% endif %}
{% endif %}
{% endfor %}
{% endfor %}
{% endif %}
[U]:unknown [R]:registered [Q]:queued [A]:active [I]:inactive [!]:requires_attention

{% endblock %}
