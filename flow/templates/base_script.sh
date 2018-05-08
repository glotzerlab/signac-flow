{% block header %}
{% endblock %}
{% block project_header %}
set -e
set -u

cd {{ project._rd }}
{% endblock %}
{% block body %}
{% for operation in operations %}
{% if parallel %}

# Operation '{{ operation.name }}' for job '{{ operation.job._id }}':
{{ operation.cmd }} &
{% else %}

# Operation '{{ operation.name }}' for job '{{ operation.job._id }}':
{{ operation.cmd }}
{% endif %}
{% endfor %}
{% endblock %}
{% block footer %}
{% if parallel %}
wait
{% endif %}
{% endblock %}
