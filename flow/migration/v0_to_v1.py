# Copyright (c) 2021 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Migrate from schema version 0 to version 1.

This migration is a null-migration that serves as a template
for future migrations and testing purposes.
"""


def migrate_v0_to_v1(project):
    """Migrate from schema version 0 to version 1."""
    pass  # nothing to do here, serves purely as an example


def migrate_v1_to_v2(project):
    """Migrate from schema version 1 to version 2."""
    # TODO: This migration is not yet implemented, but this comment documents
    # what this function will need to do. There are a few different scenarios
    # that must be accounted for:
    # 1. User updates signac before flow. In this case, signac's config API may
    #    already have changed, and we cannot rely on it. Therefore, we need add
    #    configobj as an optional requirement for flow rather than relying on
    #    signac's config API (it's available on PyPI). Alternatively, we could
    #    bundle configobj like we do in signac, but I don't want to commit to
    #    that long term so that would at best be a short term solution (say for
    #    one or two flow releases) and then after that we tell users who
    #    haven't migrated to just `pip install configobj`. Then, this function
    #    can rely on the fact that in signac schema v1 the config information
    #    is stored in signac.rc using the known schema and operate accordingly
    #    to pull it.
    # 2. User updates signac and attempts to migrate before migrating flow
    #    schema. In this case, `signac migrate` should error and tell the user
    #    to update flow and run `flow migrate` first. This can be accomplished
    #    by having the v1->v2 migration in signac check for the presence of the
    #    "flow" key in the config and error if it is present.  We will
    #    introduce the flow v1->v2 migration before signac's to ensure that
    #    this is possible.
    # 3. Users update signac and create a new project, but still use an old
    #    version of flow that has errors trying to access signac config
    #    information. This case should fail fast, and we'll just have to inform
    #    such users that they need to upgrade flow.
    #
    # Once a FlowProject has migrated to schema version 2, it is decoupled from
    # signac's internal schema and should not have any further problems.
    pass
