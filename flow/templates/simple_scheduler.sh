{% extends "base_script.sh" %}
{% block header %}
#!/bin/bash
#SSCHED --job-name={{ id }}
#SSCHED --chdir={{ project.path }}

# np_global={{ operations|calc_tasks('np', parallel, force) }}
{% endblock header %}
