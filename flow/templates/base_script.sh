{% block header %}
{% endblock %}
{% block project_header %}
set -e
set -u

cd {{ project._rd }}
{% endblock %}
{% block body %}
{% for operation in operations %}

# Operation '{{ operation.name }}' for job '{{ operation.job._id }}':
{% if parallel %}
{{ operation.cmd }} &
{% else %}
{{ operation.cmd }}
{% endif %}
{% endfor %}
{% endblock %}
{% block footer %}
{% if parallel %}
wait
{% endif %}
{% endblock %}
