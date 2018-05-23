.. _templates:

=========
Templates
=========

The **signac-flow** package uses jinja2 templates to generate scripts for execution and submission to cluster scheduling systems.
Templates for simple bash execution and submission to popular schedulers and compute clusters are shipped with the package.
To customize the script generation, a user can replace the default template or customize any of the provided ones.

Replacing the default template
==============================

The default ``script.sh`` template extends from another script according to the ``base_script`` variable, which is dynamically set by **signac-flow**:

.. literalinclude:: ../flow/templates/script.sh
   :language: jinja

This ``base_script`` variable provides a way to inject specific templates for, *e.g.*, different environments.
However, by default any templates placed within your project root directory in a file called ``templates/script.sh`` will be used instead of the defaults provided by **signac-flow**.
This makes it easy to completely replace the scripts provided by signac-flow; to use your own custom script, simply place a new ``script.sh`` in a ``templates`` directory within your project root directory.
This is an example for a basic template that would be sufficient for the simple serial execution of multiple operations:

.. code-block:: jinja

    cd {{ project.config.project_dir }}

    {% for operation in operations %}
    {{ operation.cmd }}
    {% endfor %}


Customize provided templates
============================

Instead of simply replacing the template as shown above, we can also customize the provided templates.
One major advantage is that we can still use the template scripts for cluster submission.

Assuming that we wanted to write a time stamp to some log file before executing operations, we could provide a custom template such as this one:

.. code-block:: jinja

    {% extends base_script %}
    {% block body %}
    date >> execution.log
    {{ super() }}
    {% endblock %}

The first line again indicates that this template extends an existing template based on the value of ``base_script``; how this variable is set is explained in more detail in the next section.
The second and last line indicate that the enclosed lines are to be placed in the *body* block of the base template.
The third line is the actual command that we want to add and the fourth line ensures that the code provided by the base template within the body block is still added.

The base template
=================

The **signac-flow** package will select a different base script template depending on whether you are simply generating a script using the ``script`` command or whether you are submitting to a scheduling system with ``submit``.
In the latter case, the base script template is selected based on whether you are on any of the :ref:`officially supported environments <supported-environments>`, and if not, whether one of the known scheduling system (torque or slurm) is available.
This is a short illustration of that heuristic:

.. code-block:: bash

    # The `script` command always uses the same base script template:
    project.py script --> base_script='base_script.sh'

    # On system with SLURM scheduler:
    project.py submit --> base_script='slurm.sh' (extends 'base_script.sh')

    # On XSEDE Comet
    project.py submit --> base_script='comet.sh' (extends 'slurm.sh')

Regardless of which *base script template* you are actually extending from, all templates shipped with **flow** follow the same basic structure:

.. glossary::

   resources
    Calculation of the total resources required for the execution of this (submission) script.
    The base template calculates the following variables:

      ``num_tasks``:  The total number of processing units required.  [SHOULD BE NP_GLOBAL]

   header
    Directives for the scheduling system such as the cluster job name and required resources.
    This block is empty for shell script templates.

   project_header
    Commands that should be executed once before the execution of operations, such as switching into the project root directory or setting up the software environment.

   body
    All commands required for the actual execution of operations.

   footer
    Any commands that should be executed at the very end of the script.

Execution Directives
====================

Available directives
--------------------

Any :py:class:`~flow.FlowProject` *operation* can be amended with so called *execution directives*.
For example, to specify that we want to parallelize a particular operation on **4** processing units, we would provide the ``np=4`` directive:

.. code-block:: python

    from flow import FlowProject, directives
    from multiprocessing import Pool

    @FlowProject.operation
    @directives(np=4)
    def hello(job):
        with Pool(4) as pool:
          print("hello", job)

All directives are essentially conventions, the ``np`` directive in particular means that this particular operation requires 4 processors for execution.
The following directives are respected by all base templates shipped with **signac-flow**:

.. glossary::

    np
      The total number of processing units required for this operation.

    nranks
      The number of MPI ranks required for this operation.
      The command will be prefixed with environment specific MPI command, e.g.: ``mpiexec -n 4``.
      The value for *np* will default to *nranks* unless specified separately.

    omp_num_threads
      The number of OpenMP threads.
      The value for *np* will default to: "nranks x omp_num_threads" unless otherwise specified.

    ngpu
      The number of GPUs required for this operation.

Execution Modes
---------------

Using these directives and their combinations allows us to realize the following essential execution modes:

.. glossary::

    serial:
      ``@flow.directives()``

      This operation is a simple serial process, no directive needed.

    parallelized:
      ``@flow.directives(np=4)``

      This operation requires 4 processing units.

    MPI parallelized:
      ``@flow.directives(nranks=4)``

      This operation will be executed on 4 MPI ranks.

    MPI/OpenMP Hybrid:
      ``@flow.directives(nranks=4, omp_num_threads=2)``

      This operation will be executed on 4 MPI ranks with 2 OpenMP threads per rank.

    GPU:
      ``@flow.directives(ngpu=1)``

      The operation requires one GPU for execution.


This is the body block from the ``base_script.sh`` base template:

.. literalinclude:: ../flow/templates/base_script.sh
    :language: jinja
    :lines: 19-28
