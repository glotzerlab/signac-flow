# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implementation of decorators for label functions.

This module implements the label, classlabel, and staticlabel decorator
functions which can be used to decorate label functions which are part
of a FlowProject class definition.
"""


class label:
    """Decorate a :class:`~.FlowProject` class function as a label function.

    For example:

    .. code-block:: python

        class MyProject(FlowProject):

            @label()
            def foo(self, job):
                return True
    """

    def __init__(self, name=None):
        self.name = name

    def __call__(self, func):
        """Add the function as a label.

        This call operator allows the class to be used as a decorator.

        Parameters
        ----------
        func : callable
            The function to decorate.

        Returns
        -------
        callable
            The decorated function.

        """
        func._label = True
        if self.name is not None:
            func._label_name = self.name
        return func


class staticlabel(label):
    """A label decorator for staticmethods.

    This decorator implies "staticmethod"!
    """

    def __call__(self, func):
        """Add the function as a label.

        This call operator allows the class to be used as a decorator.

        Parameters
        ----------
        func : callable
            The function to decorate.

        Returns
        -------
        callable
            The decorated function.

        """
        return staticmethod(super().__call__(func))


class classlabel(label):
    """A label decorator for classmethods.

    This decorator implies "classmethod"!
    """

    def __call__(self, func):
        """Add the function as a label.

        This call operator allows the class to be used as a decorator.

        Parameters
        ----------
        func : callable
            The function to decorate.

        Returns
        -------
        callable
            The decorated function.

        """
        return classmethod(super().__call__(func))


def _is_label_func(func):
    return getattr(getattr(func, "__func__", func), "_label", False)
