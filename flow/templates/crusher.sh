{# Templated in accordance with: https://docs.olcf.ornl.gov/systems/crusher_quick_start_guide.html #}
{% extends "slurm.sh" %}
{% block tasks %}
    {% set threshold = 0 if force else 0.9 %}
    {% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
    {% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
    {% if not gpu_tasks %}
        {% raise "Template requires GPU jobs." %}
    {% endif %}
    {% set nn = gpu_tasks|calc_num_nodes(cpu_tasks, threshold) %}
#SBATCH -N {{ nn }}
{% endblock tasks %}
{% block header %}
    {{- super() -}}
    {% set account = account|default(project|get_account_name, true) %}
    {% if account %}
#SBATCH -A {{ account }}
    {% endif %}
#SBATCH -p batch
#SBATCH --threads-per-core=2
{% endblock header %}
