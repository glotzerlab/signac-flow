{% extends "base_script.sh" %}
{% block header %}
    {% block preamble %}
#PBS -N {{ id }}
        {% set memory_requested = operations | calc_memory(parallel) %}
        {% if memory_requested %}
#PBS -l mem={{ memory_requested|format_memory }}B
        {% endif %}
        {% set walltime = operations | calc_walltime(parallel) %}
        {% if walltime %}
#PBS -l walltime={{ walltime|format_timedelta }}
        {% endif %}
        {% if job_output %}
#PBS -o {{ job_output }}
#PBS -e {{ job_output }}
        {% endif %}
        {% if not no_copy_env %}
#PBS -V
        {% endif %}
    {% endblock preamble %}
    {% block tasks %}
        {% set threshold = 0 if force else 0.9 %}
        {% set cpu_tasks = operations|calc_tasks('np', parallel, force) %}
        {% set gpu_tasks = operations|calc_tasks('ngpu', parallel, force) %}
        {% set s_gpu = ':gpus=1' if gpu_tasks else '' %}
        {% set ppn = ppn|default(operations|calc_tasks('omp_num_threads', parallel, force), true) %}
        {% if ppn %}
#PBS -l nodes={{ nn|default(cpu_tasks|calc_num_nodes(ppn, threshold, 'CPU'), true) }}:ppn={{ ppn }}{{ s_gpu }}
        {% else %}
#PBS -l procs={{ cpu_tasks }}{{ s_gpu }}
        {% endif %}
    {% endblock tasks %}
{% endblock header %}
