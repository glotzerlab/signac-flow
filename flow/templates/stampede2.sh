{% extends "slurm.sh" %}
{% block tasks %}
{% set tpo = [] %}
{% for op in operations %}
{% if op.directives.nranks and op.directives.nranks > op.directives.np %}
{% raise "Cannot request more threads (%d) than total processes (%d) for %s(%s)"|format(op.directives.nranks, op.directives.np, op.name, op.job._id) %}
{% endif %}
{%if tpo.append(op.directives.nranks|default(op.directives.np)) %}{% endif %}
{% endfor %}
{% set tasks = tpo|sum if parallel else tpo|max %}
{% set cpn = 48 if 'skx' in partition else 68 %}
{% set nn = nn|default((np_global/cpn)|round(method='ceil')|int, true) %}
{% set node_util = np_global / (nn * cpn) %}
{% if not force and node_util < 0.9 %}
{% raise "Bad node utilization!! nn=%d, cores_per_node=%d, np_global=%d"|format(nn, cpn, np_global) %}
{% endif %}
#SBATCH --nodes={{ nn }}
#SBATCH --ntasks={{ tasks }}
{% endblock %}

{% block header %}
{{ super () -}}
{% set account = 'account'|get_config_value(ns=environment) %}
{% if account %}
#SBATCH -A {{ account }}
{% endif %}
#SBATCH --partition={{ partition }}
{% endblock %}

{% block body %}
{% set cmd_suffix = cmd_suffix|default('') ~ (' &' if parallel else '') %}
{% for operation in (operations|with_np_offset) %}

# {{ "%s"|format(operation) }}
{% if operation.directives.omp_num_threads %}
export OMP_NUM_THREADS={{ operation.directives.omp_num_threads }}
{% endif %}
{% if operation.directives.nranks %}
{% if parallel %}
{% set mpi_prefix = "ibrun -n %d -o %d task_affinity "|format(operation.directives.nranks, operation.directives.np_offset) %}
{% else %}
{% set mpi_prefix = "ibrun -n %d "|format(operation.directives.nranks) %}
{% endif %}
{% endif %}
{{ mpi_prefix }}{{ cmd_prefix }}{{ operation.cmd }}{{ cmd_suffix }}
{% endfor %}
{% endblock %}
