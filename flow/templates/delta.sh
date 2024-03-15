{# Templated in accordance with: https://https://wiki.ncsa.illinois.edu/display/DSC/Delta+User+Guide #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% if partition in ["gpuA100x8", "gpuMI100x8"] %}
        {% raise "This partition is not supported as it has few nodes,
                  increased charges and is expected to be suitable for a
                  minority of use cases." %}
    {% endif %}
    {{- super() -}}
{% endblock tasks %}
