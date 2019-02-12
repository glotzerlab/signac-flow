# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements API and code paths for the legacy templating system.

Will be removed beginning version 0.8.
"""
import sys
import io
import warnings
import functools

from signac.common import six


NUM_NODES_WARNING = """Unable to determine the reqired number of nodes (nn) for this submission.
Either provide this value directly with '--nn' or provide the number of processors
per node: '--ppn'.

Please note, you can ignore this message by specifying extra submission options
    with '--' or by using the '--force' option."""


UTILIZATION_WARNING = """You either specified or the environment is configured to use {ppn}
processors per node (ppn), however you only use {usage:0.2%} of each node.
Consider to increase the number of processors per operation (--np)
or adjust the processors per node (--ppn).

Alternatively, you can also use --force to ignore this warning.
"""


# Global variable that is used internally to keep track of which
# FlowProject methods belong to the legacy templating system. Such
# a method is docorated with the _part_of_legacy_template_system()
# decorator and then registered in this variable.
_LEGACY_TEMPLATING_METHODS = set()


def _part_of_legacy_template_system(method):
    "Label a method to be part of the legacy templating system."
    _LEGACY_TEMPLATING_METHODS.add(method.__name__)
    method._legacy_intact = True
    return method


class FlowProjectLegacyTemplatingSystem(object):

    def __init__(self, *args, **kwargs):
        super(FlowProjectLegacyTemplatingSystem, self).__init__(*args, **kwargs)
        self._setup_legacy_templating()

    def _setup_legacy_templating(self):
        """This function identifies whether a subclass has implemented deprecated template
        functions.

        The legacy templating system is used to generate run and cluster submission scripts
        if that is the case. A warning is emitted to inform the user that they will not be
        able to use the standard templating system.

        The legacy templating functions are decorated with the _part_of_legacy_template_system()
        decorator.
        """
        self._legacy_templating = False
        legacy_methods = set()
        for method in _LEGACY_TEMPLATING_METHODS:
            if hasattr(self, method) and not hasattr(getattr(self, method), '_legacy_intact'):
                warnings.warn(
                    "The use of FlowProject method '{}' is deprecated!".format(method),
                    DeprecationWarning)
                legacy_methods.add(method)
        if legacy_methods:
            self._legacy_templating = True
            warnings.warn(
                "You are using the following deprecated templating methods: {}. Please remove "
                "those methods from your project class implementation to use the jinja2 templating "
                "system (version >= 0.6).".format(', '.join(legacy_methods)))

    @_part_of_legacy_template_system
    def write_script_header(self, script, **kwargs):
        """"Write the script header for the execution script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        # Add some whitespace
        script.writeline()
        # Don't use uninitialized environment variables.
        script.writeline('set -u')
        # Exit on errors.
        script.writeline('set -e')
        # Switch into the project root directory
        script.writeline('cd {}'.format(self.root_directory()))
        script.writeline()

    @_part_of_legacy_template_system
    def write_script_operations(self, script, operations, background=False, **kwargs):
        """Write the commands for the execution of operations as part of a script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        from .util.misc import write_human_readable_statepoint

        for op in operations:
            write_human_readable_statepoint(script, op.job)
            script.write_cmd(op.cmd.format(job=op.job), bg=background)
            script.writeline()

    @classmethod
    @_part_of_legacy_template_system
    def write_human_readable_statepoint(cls, script, job):
        """Write statepoint of job in human-readable format to script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        raise RuntimeError(
            "The write_human_readable_statepoint() function has been deprecated "
            "as of version 0.6 and been removed as of version 0.7.")

    @_part_of_legacy_template_system
    def write_script_footer(self, script, **kwargs):
        """"Write the script footer for the execution script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        # Wait until all processes have finished
        script.writeline('wait')

    @_part_of_legacy_template_system
    def write_script(self, script, operations, background=False, **kwargs):
        """Write a script for the execution of operations.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.

        By default, this function will generate a script with the following components:

        .. code-block:: python

            write_script_header(script)
            write_script_operations(script, operations, background=background)
            write_script_footer(script)

        :param script:
            The script to write the commands to.
        :param operations:
            The operations to be written to the script.
        :type operations:
            A sequence of JobOperation
        :param background:
            Whether operations should be executed in the background;
            useful to parallelize execution.
        :type background:
            bool
        """
        self.write_script_header(script, **kwargs)
        self.write_script_operations(script, operations, background=background, **kwargs)
        self.write_script_footer(script, **kwargs)


def script_support_legacy_templating_system(func):

    @functools.wraps(func)
    def wrapper(self, operations, parallel=False, template='script.sh', *args, **kwargs):
        if self._legacy_templating:
            import os
            from .environment import TestEnvironment
            # We first check whether it appears that the user has provided a templating script
            # in which case we raise an exception to avoid highly unexpected behavior.
            fn_template = os.path.join(self.root_directory(), 'templates', template)
            if os.path.isfile(fn_template):
                raise RuntimeError(
                    "In legacy templating mode, unable to use template '{}'.".format(fn_template))
            script = TestEnvironment.script()
            self.write_script(script, operations, background=parallel)
            script.seek(0)
            return script.read()
        else:
            return func(self, operations=operations, parallel=parallel, template=template,
                        *args, **kwargs)

    return wrapper


def _generate_submit_script_support_legacy_templating_system(func):

    @functools.wraps(func)
    def wrapper(self, _id, operations, template, show_template_help, env, *args, **kwargs):
        import os
        if self._legacy_templating:
            fn_template = os.path.join(self._template_dir, template)
            if os.path.isfile(fn_template):
                raise RuntimeError(
                    "In legacy templating mode, unable to use template '{}'.".format(fn_template))

            # For maintaining backwards compatibility for Versions<0.7
            if kwargs['parallel']:
                np_total = sum(op.directives['np'] for op in operations)
            else:
                np_total = max(op.directives['np'] for op in operations)

            script = env.script(_id=_id, np_total=np_total, **kwargs)
            background = kwargs.pop('parallel', not kwargs.pop('serial', False))
            self.write_script(script=script, operations=operations, background=background, **kwargs)
            script.seek(0)
            return script.read()
        else:
            return func(self, _id, operations, template, show_template_help, env, *args, **kwargs)

    return wrapper


class JobScript(io.StringIO):
    """"Simple StringIO wrapper for the creation of job submission scripts.

    Using this class to write a job submission script allows us to use
    environment specific expressions, for example for MPI commands.
    """
    eol = '\n'

    def __init__(self, env):
        self._env = env
        super(JobScript, self).__init__()

    def __str__(self):
        self.seek(0)
        return self.read()

    def write(self, s):
        if six.PY2:
            super(JobScript, self).write(unicode(s))  # noqa
        else:
            super(JobScript, self).write(s)

    def writeline(self, line=''):
        "Write one line to the job script."
        self.write(line + self.eol)

    def write_cmd(self, cmd, bg=False, np=None):
        """Write a command to the jobscript.

        This command wrapper function is a convenience function, which
        adds mpi and other directives whenever necessary.

        :param cmd: The command to write to the jobscript.
        :type cmd: str
        :param np: The number of processors required for execution.
        :type np: int
        """
        if np is not None:
            warnings.warn(DeprecationWarning("Do not provide np with write_cmd()!"))
            if np > 1:
                cmd = self._env.mpi_cmd(cmd, np=np)
        if bg:
            cmd = self._env.bg(cmd)
        self.writeline(cmd)


def deprecated_since_06(func):

    def wrapper(cls, *args, **kwargs):
        warnings.warn(
            "Function '{}.{}()' is part of the legacy templating system, "
            "which was deprecated as of version 0.6 and will be completely "
            "removed in version 0.7. Please upgrade your scripts!".format(
                    cls.__name__, func.__name__), DeprecationWarning)
        return func(cls, *args, **kwargs)
    return wrapper


class LegacyComputeEnvironment(object):

    @classmethod
    def script(cls, **kwargs):
        """Return a JobScript instance.

        Derived ComputeEnvironment classes may require additional
        arguments for the creation of a job submission script.
        """
        warnings.warn(
            "The ComputeEnvironment.script() function is part of the legacy templating system, "
            "which was deprecated in version 0.6 and will be removed completely in version 0.8!",
            DeprecationWarning)
        return JobScript(cls)

    @staticmethod
    def bg(cmd):
        "Wrap a command (cmd) to be executed in the background."
        warnings.warn(
            "The ComputeEnvironment.bg() function is part of the legacy templating system, "
            "which was deprecated in version 0.6 and will be removed completely in version 0.8!",
            DeprecationWarning)
        return cmd + ' &'


def support_legacy_templating_submit(func):

    @functools.wraps(func)
    def wrapper(self, script, *args, **kwargs):
        if isinstance(script, JobScript):  # api version < 6
            script = str(script)
        return func(self, script, *args, **kwargs)

    return wrapper


def support_legacy_templating(cls):

    if cls.__name__ == 'TestEnvironment':

        @classmethod
        @deprecated_since_06
        def mpi_cmd(cls, cmd, np):
            return 'mpirun -np {np} {cmd}'.format(np=np, cmd=cmd)
        cls.mpi_cmd = mpi_cmd

        @classmethod
        @deprecated_since_06
        def script(cls, **kwargs):
            js = super(cls, cls).script(**kwargs)
            for key in sorted(kwargs):
                js.writeline('#TEST {}={}'.format(key, kwargs[key]))
            return js
        cls.script = script

    elif cls.__name__ == 'NodesEnvironment':
        from .errors import SubmitError

        @classmethod
        @deprecated_since_06
        def calc_num_nodes(cls, np_total, ppn, force=False, **kwargs):
            from math import ceil
            if ppn is None:
                try:
                    ppn = getattr(cls, 'cores_per_node')
                except AttributeError:
                    raise SubmitError(NUM_NODES_WARNING)

            # Calculate the total number of required nodes
            nn = int(ceil(np_total / ppn))

            if not force:  # Perform basic check concerning the node utilization.
                usage = np_total / nn / ppn
                if usage < 0.9:
                    print(UTILIZATION_WARNING.format(ppn=ppn, usage=usage), file=sys.stderr)
                    raise SubmitError("Bad node utilization!")
            return nn
        cls.calc_num_nodes = calc_num_nodes

        @classmethod
        @deprecated_since_06
        def add_args(cls, parser):
            super(cls, cls).add_args(parser)
            parser.add_argument(
                '--nn',
                type=int,
                help="Specify the total number of nodes. This value is computed automatically "
                     "from the 'np' directive otherwise.")
        cls.add_args = add_args

    elif cls.__name__ == 'DefaultTorqueEnvironment':

        @classmethod
        @deprecated_since_06
        def mpi_cmd(cls, cmd, np):
            return 'mpirun -np {np} {cmd}'.format(np=np, cmd=cmd)
        cls.mpi_cmd = mpi_cmd

        @classmethod
        @deprecated_since_06
        def gen_tasks(cls, js, np_total):
            """Helper function to generate the number of tasks (for overriding)"""
            js.writeline('#PBS -l nodes={}'.format(np_total))
            return js
        cls.gen_tasks = gen_tasks

        @classmethod
        @deprecated_since_06
        def script(cls, _id, np_total, walltime=None, no_copy_env=False, **kwargs):
            from .util.template_filters import format_timedelta
            js = super(cls, cls).script()
            js.writeline('#PBS -N {}'.format(_id))
            js = cls.gen_tasks(js)
            if walltime is not None:
                js.writeline('#PBS -l walltime={}'.format(format_timedelta(walltime)))
            if not no_copy_env:
                js.writeline('#PBS -V')
            return js
        cls.script = script

    elif cls.__name__ == 'DefaultSlurmEnvironment':

        @classmethod
        @deprecated_since_06
        def mpi_cmd(cls, cmd, np):
            return 'mpirun -np {np} {cmd}'.format(np=np, cmd=cmd)
        cls.mpi_cmd = mpi_cmd

        @classmethod
        @deprecated_since_06
        def gen_tasks(cls, js, np_total):
            """Helper function to generate the number of tasks (for overriding)"""
            js.writeline('#SBATCH --ntasks={}'.format(np_total))
            return js
        cls.gen_tasks = gen_tasks

        @classmethod
        @deprecated_since_06
        def script(cls, _id, np_total, walltime=None, **kwargs):
            from .util.template_filters import format_timedelta
            js = super(cls, cls).script()
            js.writeline('#!/bin/bash')
            js.writeline('#SBATCH --job-name="{}"'.format(_id))
            js = cls.gen_tasks(js, np_total)
            if walltime is not None:
                js.writeline('#SBATCH -t {}'.format(format_timedelta(walltime)))
            return js
        cls.script = script

    elif cls.__name__ == 'DefaultLSFEnvironment':

        @classmethod
        @deprecated_since_06
        def mpi_cmd(cls, cmd, np):
            raise NotImplementedError("LSF environments are not supported by the "
                                      "legacy templating system.")
        cls.mpi_cmd = mpi_cmd

        @classmethod
        @deprecated_since_06
        def gen_tasks(cls, js, np_total):
            raise NotImplementedError("LSF environments are not supported by the "
                                      "legacy templating system.")
        cls.gen_tasks = gen_tasks

        @classmethod
        @deprecated_since_06
        def script(cls, _id, np_total, walltime=None, **kwargs):
            raise NotImplementedError("LSF environments are not supported by the "
                                      "legacy templating system.")
        cls.script = script

    else:
        raise TypeError("No legacy templating defined for '{}'.".format(cls))

    return cls
