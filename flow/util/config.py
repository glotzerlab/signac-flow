# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Contains logic for working with flow related information
stored in the signac config"""

from signac.common import config

from ..errors import ConfigKeyError


class _GetConfigValueNoneType(object):
    pass


_GET_CONFIG_VALUE_NONE = _GetConfigValueNoneType()


MISSING_ENV_CONF_KEY_MSG = """Your environment is missing the following configuration key: '{key}'
Please provide the missing information, for example by adding it to your global configuration:

signac config --global set {key} <VALUE>
"""


def require_config_value(key, ns=None, default=_GET_CONFIG_VALUE_NONE):
    """Request a value from the user's configuration, fail if not available.

    This method should be used whenever values need to be provided
    that are specific to a users's environment. A good example are
    account names.

    When a key is not configured and no default value is provided,
    a :py:class:`~.errors.SubmitError` will be raised and the user will
    be prompted to add the missing key to their configuration.

    Please note, that the key will be automatically expanded to
    be specific to this environment definition. For example, a
    key should be 'account', not 'MyEnvironment.account`.

    :param key: The environment specific configuration key.
    :type key: str
    :param ns: The namespace in which to look for the key
    :type key: str
    :param default: A default value in case the key cannot be found
        within the user's configuration.
    :return: The value or default value.
    :raises ConfigKeyError: If the key is not in the user's configuration
        and no default value is provided.
    """
    try:
        if ns is None:
            return config.load_config()['flow'][key]
        else:
            return config.load_config()['flow'][ns][key]
    except KeyError:
        if default is _GET_CONFIG_VALUE_NONE:
            k = str(key) if ns is None else '{}.{}'.format(ns, key)
            print(MISSING_ENV_CONF_KEY_MSG.format(key='flow.' + k))
            raise ConfigKeyError("Missing environment configuration key: '{}'".format(k))
        else:
            return default


def get_config_value(key, ns=None, default=None):
    """Request a value from the user's configuration.

    This method should be used whenever values need to be provided
    that are specific to a users's environment. A good example are
    account names.

    When a key is not configured and no default value is provided,
    a :py:class:`~.errors.SubmitError` will be raised and the user will
    be prompted to add the missing key to their configuration.

    Please note, that the key will be automatically expanded to
    be specific to this environment definition. For example, a
    key should be 'account', not 'MyEnvironment.account`.

    :param key: The environment specific configuration key.
    :type key: str
    :param ns: The namespace in which to look for the key
    :type key: str
    :param default: A default value in case the key cannot be found
        within the user's configuration.
    :return: The value if found, None if not found.
    """
    try:
        if ns is None:
            return config.load_config()['flow'][key]
        else:
            return config.load_config()['flow'][ns][key]
    except KeyError:
        return default
