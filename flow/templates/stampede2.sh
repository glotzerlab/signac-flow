{% extends "slurm.sh" %}
{% block tasks %}
{% set tpo = [] %}
{% for op in operations %}
{% if op.directives.tasks and op.directives.tasks > op.directives.np %}
{% raise "Cannot request more threads (%d) than total processes (%d) for %s(%s)"|format(op.directives.tasks, op.directives.np, op.name, op.job._id) %}
{% endif %}
{%if tpo.append(op.directives.tasks|default(op.directives.np)) %}{% endif %}
{% endfor %}
{% set tasks = tpo|sum if parallel else tpo|max %}
{% set cpn = 48 if 'skx' in partition else 68 %}
{% set nn = nn|default((num_tasks/cpn)|round(method='ceil')|int, true) %}
{% set node_util = num_tasks / (nn * cpn) %}
{% if not force and node_util < 0.9 %}
{% raise "Bad node utilization!! nn=%d, cores_per_node=%d, num_tasks=%d"|format(nn, cpn, num_tasks) %}
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

{# On stampede we default to using ibrun; extend and override the template if needed #}
{% block body %}
{% if parallel %}
{% for operation in (operations|with_np_offset) %}
# Operation '{{ operation.name }}' for job '{{ operation.job._id }}':
{{ "ibrun -n %d -o %d %s &"|format(operation.directives.np, operation.directives.np_offset, operation.cmd) }}
{% endfor %}
{% else %}
{% for operation in operations %}
{{ "ibrun -n %d %s"|format(operation.directives.np, operation.cmd) }}
{% endfor %}
{% endif %}
{% endblock %}
{% block footer %}
{% if parallel %}
wait
{% endif %}
{% endblock %}
