{# Templated in accordance with: https://portal.tacc.utexas.edu/user-guides/stampede2 #}
{% extends "slurm.sh" %}
{% set ns = namespace(use_launcher=True) %}
{% if operations|length() < 2 %}
{% set ns.use_launcher = False %}
{% endif %}
{% for operation in operations %}
{% if operation.directives.nranks or operation.directives.omp_num_threads or operation.directives.np > 1 %}
{% set ns.use_launcher = False %}
{% endif %}
{% endfor %}

{% block tasks %}
{% set threshold = 0 if force else 0.9 %}
{% if operations|calc_tasks('ngpu', false, true) and not force %}
{% raise "GPUs were requested but are unsupported by Stampede2!" %}
{% endif %}
{% set cpn = 48 if 'skx' in partition else 68 %}
{% if ns.use_launcher %}
{% set cpu_tasks = operations|calc_tasks('np', true, force) %}
#SBATCH --nodes={{ nn|default(cpu_tasks|calc_num_nodes(cpn, threshold, 'CPU'), true) }}
#SBATCH --ntasks={{ nn|default(cpu_tasks|calc_num_nodes(cpn, threshold, 'CPU'), true) * cpn }}
{% else %}
{% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
#SBATCH --nodes={{ nn|default(cpu_tasks|calc_num_nodes(cpn, threshold, 'CPU'), true) }}
#SBATCH --ntasks={{ (operations|calc_tasks('nranks', parallel, force), 1)|max }}
{% endif %}
{% endblock %}

{% block header %}
{{ super () -}}
{% set account = account|default(environment|get_account_name, true) %}
{% if account %}
#SBATCH -A {{ account }}
{% endif %}
{% endblock %}

{% block body %}
{% if ns.use_launcher %}
{% if parallel %}
{{("Bundled submission without MPI on Stampede2 is using launcher; the --parallel option is therefore ignored.")|print_warning}}
{% endif %}
{% set launcher_file = 'launcher_' ~ id|replace('/', '_') %}
{% set cmd_suffix = cmd_suffix|default('') %}
cat << EOF > {{ launcher_file }}
{% for operation in (operations|with_np_offset) %}
{{ operation.cmd }}{{ cmd_suffix }}
{% endfor %}
EOF

export LAUNCHER_PLUGIN_DIR=$LAUNCHER_DIR/plugins
export LAUNCHER_RMI=SLURM
export LAUNCHER_JOB_FILE={{ launcher_file }}

$LAUNCHER_DIR/paramrun
rm {{ launcher_file }}
{% else %}
{% set cmd_suffix = cmd_suffix|default('') ~ (' &' if parallel else '') %}
{% for operation in operations %}

# {{ "%s"|format(operation) }}
{{ "_FLOW_STAMPEDE_OFFSET_=%d "|format(operation.directives['nranks']|return_and_increment) }}{{ operation.cmd }}{{ cmd_suffix }}
{% if operation.eligible_operations|length > 0 %}
# Eligible to run:
{% for run_op in operation.eligible_operations %}
# {{ run_op.cmd }}
{% endfor %}
{% endif %}
{% if operation.operations_with_unmet_preconditions|length > 0 %}
# Operations with unmet preconditions:
{% for run_op in operation.operations_with_unmet_preconditions %}
# {{ run_op.cmd }}
{% endfor %}
{% endif %}
{% if operation.operations_with_met_postconditions|length > 0 %}
# Operations with all postconditions met:
{% for run_op in operation.operations_with_met_postconditions %}
# {{ run_op.cmd }}
{% endfor %}
{% endif %}
{% endfor %}
{# We need to reset the environment's base offset in between script generation for separate bundles. #}
{# Since Jinja's bytecode optimizes out calls to filters with a constant argument, we are forced to #}
{# rerun this function on the environment's base offset at the end of each run to return the offset to 0. #}
{{ "%d"|format(environment.base_offset)|decrement_offset }}
{% endif %}
{% endblock %}
