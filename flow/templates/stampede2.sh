{# Templated in accordance with: https://portal.tacc.utexas.edu/user-guides/stampede2 #}
{% extends "slurm.sh" %}
{% set ns = namespace(use_mpi=false) %}
{% for operation in operations %}
{% if operation.directives.nranks or operation.directives.omp_num_threads or operation.directives.np > 1 %}
{% set ns.use_mpi = true %}
{% endif %}
{% endfor %}

{% block tasks %}
{% set threshold = 0 if force else 0.9 %}
{% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
{% if operations|calc_tasks('ngpu', false, true) and not force %}
{% raise "GPUs were requested but are unsupported by Stampede2!" %}
{% endif %}
{% set cpn = 48 if 'skx' in partition else 68 %}
#SBATCH --nodes={{ nn|default(cpu_tasks|calc_num_nodes(cpn, threshold, 'CPU'), true) }}
{% if ns.use_mpi %}
#SBATCH --ntasks={{ (operations|calc_tasks('nranks', parallel, force), 1)|max }}
{% else %}
#SBATCH --ntasks={{ cpu_tasks }}
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
{% if ns.use_mpi %}
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
{% else %}
{% if parallel %}
{{("Bundled submission without MPI on Stampede2 is using launcher; the --parallel option is therefore ignored.")|print_warning}}
{% endif %}
{% set launcher_file = 'launcher_' ~ id|replace('/', '_') %}
{% set cmd_suffix = cmd_suffix|default('') %}
cat << EOF > {{ launcher_file }}
{% for operation in (operations|with_np_offset) %}
{{ cmd_prefix }}{{ operation.cmd }}{{ cmd_suffix }}
{% endfor %}
EOF

export LAUNCHER_PLUGIN_DIR=$LAUNCHER_DIR/plugins
export LAUNCHER_RMI=SLURM
export LAUNCHER_JOB_FILE={{ launcher_file }}

$LAUNCHER_DIR/paramrun
rm {{ launcher_file }}
{% endif %}
{% endblock %}
