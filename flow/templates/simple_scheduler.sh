{% extends "base_script.sh" %}
{% block header %}
#!/bin/bash
#SSCHED --job-name={{ id }}
#SSCHED --chdir={{ project.config.project_dir }}
{% endblock %}
