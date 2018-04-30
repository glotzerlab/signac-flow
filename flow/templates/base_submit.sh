{% block header %}
{% endblock %}
{% block project_header %}
set -e
set -u

cd {{ project._rd }}
{% endblock %}
{% block body %}
{% for operation in operations %}
{{ operation.cmd }} &
{% endfor %}
{% endblock %}
{% block footer %}
wait
{% endblock %}
