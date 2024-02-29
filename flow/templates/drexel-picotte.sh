{% extends "slurm.sh" %}
{% set partition = partition|default('standard', true) %}
