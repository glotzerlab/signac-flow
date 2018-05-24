.. _changes:

=======
Changes
=======

What's new with version 0.6
===========================

Major changes
-------------

 1. The generation of execution and submission scripts is now based on the jinja2_ templating system.
 2. The new :ref:`decorator API <workflow-definition>` for the definition of a :class:`~.flow.FlowProject` class largely reduces the amount of boiler plate code needed to implement FlowProjects.
    It also removes the necessity to have at least two modules, *e.g.*, one ``project.py`` and one ``operations.py`` module.
 3. Serial execution is now the default for all project sub commands, that includes ``run``, ``script``, and ``submit``.
    Parallel execution must be explicitly enabled with the ``--parallel`` option.
 4. The ``run`` command executes **all eligible** operations, that means you don't have to run the command multiple times to "cycle" through all pending operations. Accidental infinite loops are automatically avoided.
 5. Execution scripts generated with the `script` option are always bundled.
    The previous behavior, where the script command would print multiple scripts to screen unless the ``--bundle`` option was provided did not make much sense.

See the full :ref:`changelog <changelog>` below for detailed information on all changes.

.. _jinja2: http://jinja.pocoo.org/

How to migrate existing projects
--------------------------------

If your project runs with flow 0.5 without any DeprecationWarnings (that means no messages when running Python with the -W flag), then you don't *have* to do anything. Version 0.6 is mostly backwards compatible to 0.5, with the execption of custom script templating.

Since 0.6 uses jinja2_ to generate execution and submission scripts, the previous method of generating custom scripts by overloading the ``FlowProject.write_script*()`` methods is now deprecated.
That means that if you overloaded any of these functions, the new templating system is disabled, and **flow** will fallback to the old templating system and you won't be able to use jinja2 templates.

If you decide to migrate to the new API, those are the steps you most likely have to take:

 1. Replace all ``write_script*()`` methods and replace them with a :ref:`custom template script <templates>`, *e.g.*, `templates/script.sh` within your project root directory.
 2. Optionally, use the new decorator API instead of ``FlowProject.add_operation`` to add operations to your FlowProject.
 3. Optionally, use the new decorator API to define label functions for your FlowProject.
 4. Execute your project with the Python ``-w`` option to make DeprecationWarnings visible and address all issues.

We recommend to go through the tutorial on signac-docs_ to learn how to best take advantage of **flow** 0.6.

.. _signac-docs: https://signac-docs.readthedocs.io/en/latest

.. _changelog:

.. include:: ../changelog.txt
