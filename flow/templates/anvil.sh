{# Templated in accordance with: https://www.rcac.purdue.edu/knowledge/anvil/ #}
{% extends "slurm.sh" %}
{% block header %}
    {{- super() -}}
# As of 2023-10-30, Anvil incorrectly binds ranks to cores with `mpirun -n`.
# Disable core binding to work around this issue.
export OMPI_MCA_hwloc_base_binding_policy=""
{% endblock header %}
