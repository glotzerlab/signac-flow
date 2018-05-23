{# Number of tasks is the same for any script type #}
{% if parallel %}
{% set np_global = operations|map(attribute='directives.np')|sum %}
{% else %}
{% set np_global = operations|map(attribute='directives.np')|max %}
{% endif %}
{% block header %}
{% endblock %}

{% block project_header %}
set -e
set -u

cd {{ project.config.project_dir }}
{% endblock %}
{% block body %}
{% set cmd_suffix = cmd_suffix|default('') ~ (' &' if parallel else '') %}
{% for operation in operations %}
{% if operation.directives.nranks and not mpi_prefix %}
{% set mpi_prefix = "%s -n %d "|format(mpiexec|default("mpiexec"), operation.directives.nranks) %}
{% endif %}

# {{ "%s"|format(operation) }}
{% if operation.directives.omp_num_threads %}
export OMP_NUM_THREADS={{ operation.directives.omp_num_threads }}
{% endif %}
{{ mpi_prefix }}{{ cmd_prefix }}{{ operation.cmd }}{{ cmd_suffix }}
{% endfor %}
{% endblock %}
{% block footer %}
{% if parallel %}
wait
{% endif %}
{% endblock %}
