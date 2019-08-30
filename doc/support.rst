.. _support:

=======================
Support and Development
=======================

Getting help and reporting issues
=================================

To get help using the **signac-flow** package, either send an email to `signac-support@umich.edu <mailto:signac-support@umich.edu>`_ or join the `signac gitter chatroom <https://gitter.im/signac/Lobby>`_.

The **signac-flow** package is hosted on `GitHub <https://github.com/glotzerlab/signac-flow>`_ and licensed under the open-source BSD 3-Clause license.
Please use the `repository's issue tracker <https://github.com/glotzerlab/signac-flow/issues>`_ to report bugs or request new features.

Code contributions
==================

This project is open-source.
Users are highly encouraged to contribute directly by implementing new features and fixing issues.
Development for packages as part of the **signac** framework should follow the general development guidelines outlined `here <http://docs.signac.io/en/latest/community.html#contributions>`__.

A brief summary of contributing guidelines are outlined in the `CONTRIBUTING.md <https://github.com/glotzerlab/signac-flow/blob/master/CONTRIBUTING.md>`_ file as part of the repository.
All contributors must agree to the `Contributor Agreement <https://github.com/glotzerlab/signac-flow/blob/master/ContributorAgreement.md>`_ before their pull request can be merged.

Set up a development environment
--------------------------------

Start by `forking <https://github.com/glotzerlab/signac-flow/fork>`_ the project.


We highly recommend to setup a dedicated development environment,
for example with `venv <https://docs.python.org/3/library/venv.html>`_:

.. code-block:: bash

    ~ $ python -m venv ~/envs/signac-flow-dev
    ~ $ source ~/envs/signac-flow-dev/bin/activate
    (signac-flow-dev) ~ $ pip install -r requirements-dev.txt

or alternatively with `conda <https://conda.io/docs/>`_:

.. code-block:: bash

    ~ $ conda create -n signac-flow-dev python --file requirements-dev.txt
    ~ $ activate signac-flow-dev

Then clone your fork and install the package from source with:

.. code-block:: bash

    (signac-flow-dev) ~ $ cd path/to/my/fork/of/signac-flow
    (signac-flow-dev) signac-flow $ pip install -e .

The ``-e`` option stands for *editable*, which means that the package is directly loaded from the source code repository.
That means any changes made to the source code are immediately reflected upon reloading the Python interpreter.

Finally, we recommend to setup a `Flake8 <http://flake8.pycqa.org/en/latest/>`_ git commit hook with:

.. code-block:: bash

    (signac-flow-dev) signac-flow $ flake8 --install-hook git
    (signac-flow-dev) signac-flow $ git config --bool flake8.strict true

With the *flake8* hook, your code will be checked for syntax and style before you make a commit.
The continuous integration pipeline for the package will perform these checks as well, so running these tests before committing / pushing will prevent the pipeline from failing due to style-related issues.

The development workflow
------------------------

Prior to working on a patch, it is advisable to create an `issue <https://github.com/glotzerlab/signac-flow/issues>`_ that describes the problem or proposed feature.
This means that the code maintainers and other users get a chance to provide some input on the scope and possible limitations of the proposed changes, as well as advise on the actual implementation.

All code changes should be developed within a dedicated git branch and must all be related to each other.
Unrelated changes, such as minor fixes to unrelated bugs encountered during implementation, spelling errors, and similar typographical mistakes must be developed within a separate branch.

Branches should be named after the following pattern: ``<prefix>/issue-<#>-optional-short-description``.
Choose from one of the following prefixes depending on the type of change:

  * ``fix/``: Any changes that fix the code and documentation.
  * ``feature/``: Any changes that introduce a new feature.
  * ``release/``: Reserved for release branches.

If your change does not seem to fall into any of the above mentioned categories, use ``misc/``.

Once you are content with your changes, push the new branch to your forked repository and create a pull request into the main repository.
Feel free to push a branch before completion to get input from the maintainers and other users, but make sure to add a comment that clarifies that the branch is not ready for merge yet.

Testing
-------

Prior to fixing an issue, implement unit tests that *fail* for the described problem.
New features must be tested with unit and integration tests.
To run tests, execute:

.. code-block:: bash

    (signac-flow-dev) signac-flow $ python -m unittest discover tests/


Building documentation
----------------------

Building documentation requires the `sphinx <http://www.sphinx-doc.org/en/master/>`_ package which you will need to install into your development environment.

.. code-block:: bash

   (signac-flow-dev) signac-flow $ pip install Sphinx sphinx_rtd_theme

Then you can build the documentation from within the ``doc/`` directory as part of the source code repository:

.. code-block:: bash

    (signac-flow-dev) signac-flow $ cd doc/
    (signac-flow-dev) doc $ make html

.. note::

    Documentation as part of the package should be largely limited to the API.
    More elaborate documentation on how to integrate **signac-flow** into a computational workflow should be documented as part of the `framework documentation <https://docs.signac.io>`_, which is maintained `here <https://github.com/glotzerlab/signac-docs>`__.


Updating the changelog
----------------------

To update the changelog, add a one-line description to the `changelog.txt <https://docs.signac.io/projects/flow/en/latest/changes.html>`_ file within the ``next`` section.
For example:

.. code-block:: bash

    next
    ----

    - Fix issue with launching rockets to the moon.

    [0.6.3] -- 2018-08-22
    ---------------------

    - Fix issue related to dynamic data spaces, ...

Just add the ``next`` section in case it doesn't exist yet.

Contributing Environments to the Package
----------------------------------------

Users are also **highly encouraged** to contribute environment profiles that they developed for their local environments.
While there are a few steps, they are almost all entirely automated, with the exception of actually reviewing the scripts your environment generates.

Before you begin the process, make sure you have the following packages installed (in addition to **signac-flow**):

  1. `python-docx <https://python-docx.readthedocs.io/en/latest/user/install.html#install>`_
  2. `GitPython <https://gitpython.readthedocs.io/en/stable/intro.html>`_

Once you've written the environment class and the template as described above, contributing the environments to the package involves the following:

  1. Create a new branch of **signac-flow** based on the *master* branch.
  2. Add your environment class to the *flow/environments/* directory, and add the corresponding template to the *flow/templates/* directory.
  3. Run the `tests/test_templates.py` test script. It should fail on your environment, indicating that no reference scripts exist yet.
  4. Update the `environments` dictionary in the `init` function of `tests/generate_template_reference_data.py`. The dictionary indicates the submission argument combinations that need to be tested for your environment.
  5. Run the `tests/generate_template_reference_data.py` script, which will create the appropriate reference data in the `tests/template_reference_data.tar.gz` tarball based on your modifications. The `test_templates.py` script should now succeed.
  6. Run the `tests/extract_templates.py` script, which will extract the tarball into a **signac** project folder.
  7. Run the `tests/generate_template_review_document.py` script, which will generate docx files in the *tests/compiled_scripts/* directory, one for each environment.
  8. You should see one named after your new environment class. **Review the generated scripts thoroughly.** This step is critical, as it ensures that the environment is correctly generating scripts for various types of submission.
  9. Once you've fixed any issues with your environment and template, push your changes and create a pull request. You're done!

.. _deprecation-policy:

Deprecation Policy
------------------

While the signac-flow API is not considered stable yet (a *1.0* release has not
been made), we apply the following deprecation policy:

Some features may be deprecated in future releases in which case the
deprecation is announced as part of the documentation, the change log, and
their use will trigger warnings.
A deprecated feature is removed in the next minor version, unless it is
considered part of the core API in which case a reasonable attempt at
maintaining backwards compatibility is made in the next minor version, but is
then completely removed in any following minor or major release.

*A feature is considered to be part of the core API if it is likely to be used by the majority of existing projects.*

A feature which is deprecated in version *0.x*, will trigger
warnings for all releases with release number *0.x.\**, and will be removed in
version *0.y.0*.
A feature, which is deprecated in version *0.x* and which is considered core
API will trigger warnings for versions *0.x.\** and *0.y.\**, limited backwards
compatibility will be maintained throughout versions *0.y.\**, and the feature
will be removed in version *0.z.0*.

**For example: A feature deprecated in version 0.6, will be removed in version 0.7, unless it is considered core API, in which case, some backwards compatibility is maintained in version 0.7, and it is removed in version 0.8.**
