{{ "{{% extends " + extend_template + " %}}" }}
{% raw %}
{% block header %}
    {{- super () -}}
{% endblock header %}
{% block body %}
    {{- super () -}}
{% endblock body %}
{% endraw %}
