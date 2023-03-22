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
    {% if resources.ngpu_tasks and not force %}
        {% raise "GPUs were requested but are unsupported by Stampede2!" %}
    {% endif %}
    {% set cpn = 48 if 'skx' in partition else 68 %}
#SBATCH --nodes={{ resources.num_nodes }}
#SBATCH --ntasks={{ resources.ncpu_tasks }}
{% endblock tasks %}

{% block header %}
    {{- super () -}}
    {% set account = account|default(project|get_account_name, true) %}
    {% if account %}
#SBATCH -A {{ account }}
    {% endif %}
{% endblock %}

{% block body %}
    {% if use_launcher %}
        {% if parallel %}
            {{ ("Bundled submission without MPI on Stampede2 is using launcher; the --parallel option is therefore ignored.")|print_warning }}
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
        {#- Only the pre_operation block is overridden, all other behavior is inherited from base_script.sh #}
        {{- super () -}}
        {#- We need to reset the environment's base offset in between script generation for separate bundles. #}
        {#- Since Jinja's bytecode optimizes out calls to filters with a constant argument, we are forced to #}
        {#- rerun this function on the environment's base offset at the end of each run to return the offset to 0. #}
        {{- "%d"|format(environment.base_offset)|decrement_offset }}
    {% endif %}
{% endblock body %}

{# This override needs to happen outside the body block above, otherwise jinja2 doesn't seem to #}
{# respect block scope and the operations variable is left undefined. #}
{% block pre_operation %}
export _FLOW_STAMPEDE_OFFSET_={{ "%d"|format(operation.directives['nranks']|return_and_increment) }}
{% endblock pre_operation %}
