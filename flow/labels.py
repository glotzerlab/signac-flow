# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.


class label(object):
    """Decorate a function to be a label function.

    The label() method as part of FlowProject iterates over all
    methods decorated with this label and yields the method's name
    or the provided name.

    For example:

    .. code::

        class MyProject(FlowProject):

            @label()
            def foo(self, job):
                return True

            @label()
            def bar(self, job):
                return 'a' in job.statepoint()

        >>> for label in MyProject().labels(job):
        ...     print(label)

    The code segment above will always print the label 'foo',
    but the label 'bar' only if 'a' is part of a job's state point.

    This enables the user to quickly write classification functions
    and use them for labeling, for example in the classify() method.
    """

    def __init__(self, name=None):
        self.name = name

    def __call__(self, func):
        func._label = True
        if self.name is not None:
            func._label_name = self.name
        return func


class staticlabel(label):
    """A label decorator for staticmethods.

    This decorator implies "staticmethod"!
    """

    def __call__(self, func):
        return staticmethod(super(staticlabel, self).__call__(func))


class classlabel(label):
    """A label decorator for classmethods.

    This decorator implies "classmethod"!
    """

    def __call__(self, func):
        return classmethod(super(classlabel, self).__call__(func))


def _is_label_func(func):
    return getattr(getattr(func, '__func__', func), '_label', False)
