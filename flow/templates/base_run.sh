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
