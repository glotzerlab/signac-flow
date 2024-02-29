{# Templated in accordance with: https://docs.olcf.ornl.gov/systems/andes_user_guide.html #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% if 'gpu' in partition %}
        {% if resources.cpus > resources.gpus * 14 and not force %}
            {% raise "Cannot request more than 14 CPUs per GPU." %}
        {% endif %}
    {% endif %}
    {{- super() -}}
{% endblock tasks %}
