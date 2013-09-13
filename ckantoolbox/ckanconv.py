#! /usr/bin/env python
# -*- coding: utf-8 -*-


# CKAN-Toolbox -- Various modules that handle CKAN API and data
# By: Emmanuel Raviart <emmanuel@raviart.com>
#
# Copyright (C) 2013 Emmanuel Raviart
# http://github.com/etalab/ckan-toolbox
#
# This file is part of CKAN-Toolbox.
#
# CKAN-Toolbox is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# CKAN-Toolbox is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


"""Validators and converters for CKAN data"""


from biryani1.baseconv import (
    cleanup_line,
    cleanup_text,
    condition,
    default,
    empty_to_none,
    first_match,
    input_to_email,
    input_to_int,
    make_input_to_url,
    not_none,
    pipe,
    struct,
    test_equals,
    test_greater_or_equal,
    test_in,
    test_isinstance,
    translate,
    uniform_sequence,
    )
from biryani1.datetimeconv import (
    date_to_iso8601_str,
    datetime_to_iso8601_str,
    iso8601_input_to_date,
    iso8601_input_to_datetime,
    )


def ckan_input_package_to_output_package(package, state = None):
    if package is None:
        return package, None
    errors = {}
    package = package.copy()

    for key in (
            'capacity',
            'id',
            'isopen',
            'license_title',
            'license_url',
            'num_resources',
            'num_tags',
            'private',
            'revision_id',
            'state',
            'tracking_summary',
            ):
        package.pop(key, None)

    # Remove useless items from extras.
    extras = [
        dict(
            key = extra['key'],
            value = extra['value'],
            )
        for extra in (package.get('extras') or [])
        if extra.get('value') is not None
        ]
    if extras:
        package['extras'] = extras
    else:
        package.pop('extras', None)

    resources, resources_error = uniform_sequence(ckan_input_resource_to_output_resource)(
        package.get('resources'),state = state)
    if resources_error is not None:
        errors['resources'] = resources_error
    if resources:
        package['resources'] = resources
    else:
        package.pop('resources', None)

    # Remove useless items from tags.
    tags = [
        dict(name = tag['name'])
        for tag in (package.get('tags') or [])
        ]
    if tags:
        package['tags'] = tags
    else:
        package.pop('tags', None)

    return package, errors or None


def ckan_input_resource_to_output_resource(resource, state = None):
    if resource is None:
        return resource, None
    errors = {}
    resource = resource.copy()

    for key in (
            'cache_last_updated',
            'cache_url',
            'cache_url_updated',
            'hash',
            'id',
            'owner',
            'position',
            'resource_group_id',
            'revision_id',
            'size',
            'state',
            'tracking_summary',
            'url_type',
            ):
        resource.pop(key, None)

    return resource, errors or None


ckan_json_to_approval_status = pipe(
    test_isinstance(basestring),
    test_in([u'approved', u'pending']),
    )


ckan_json_to_group_type = pipe(
    test_isinstance(basestring),
    test_in([u'group', u'organization', u'service']),
    )


ckan_json_to_id = test_isinstance(basestring)


ckan_json_to_iso8601_date_str = pipe(
    test_isinstance(basestring),
    iso8601_input_to_date,
    date_to_iso8601_str,
    )


ckan_json_to_iso8601_datetime_str = pipe(
    test_isinstance(basestring),
    iso8601_input_to_datetime,
    datetime_to_iso8601_str,
    )


ckan_json_to_name_list = pipe(
    test_isinstance(list),
    uniform_sequence(
        pipe(
            test_isinstance(basestring),
            empty_to_none,
            not_none,
            ),
        ),
    not_none,
    empty_to_none,
    )


ckan_json_to_state = pipe(
    test_isinstance(basestring),
    test_in([u'active', u'deleted']),
    )


def make_ckan_json_to_datastore(drop_none_values = False, keep_value_order = False, skip_missing_items = False):
    return pipe(
        test_isinstance(dict),
        struct(
            dict(
                fields = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            test_isinstance(dict),
                            struct(
                                dict(
                                    id = pipe(
                                        test_isinstance(basestring),
                                        not_none,
                                        ),
                                    type = pipe(
                                        test_isinstance(basestring),
                                        not_none,
                                        ),
                                    ),
                                ),
                            not_none,
                            ),
                        ),
                    not_none,
                    empty_to_none,
                    ),
                method = pipe(
                    test_isinstance(basestring),
                    test_equals(u'insert'),
                    not_none,
                    ),
                primary_key = pipe(
                    test_isinstance(basestring),
                    not_none,
                    ),
                resource_id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                ),
            drop_none_values = drop_none_values,
            keep_value_order = keep_value_order,
            skip_missing_items = skip_missing_items,
            ),
        )


def make_ckan_json_to_embedded_group(drop_none_values = False, keep_value_order = False, skip_missing_items = False):
    return pipe(
        test_isinstance(dict),
        struct(
            dict(
                approval_status = ckan_json_to_approval_status,
                capacity = pipe(
                    test_isinstance(basestring),
                    test_in([u'private', u'public']),
                    ),
                created = ckan_json_to_iso8601_datetime_str,
                description = pipe(
                    test_isinstance(basestring),
                    cleanup_text,
                    ),
                id = ckan_json_to_id,
                image_url = pipe(
                    test_isinstance(basestring),
                    make_input_to_url(add_prefix = u'http://', full = True),
                    ),
                name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
                    ),
                revision_id = ckan_json_to_id,
                state = ckan_json_to_state,
                title = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                type = ckan_json_to_group_type,
                ),
            drop_none_values = drop_none_values,
            keep_value_order = keep_value_order,
            skip_missing_items = skip_missing_items,
            ),
        )


def make_ckan_json_to_embedded_package(drop_none_values = False, keep_value_order = False, skip_missing_items = False):
    return pipe(
        test_isinstance(dict),
        struct(
            dict(
                author = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                author_email = pipe(
                    test_isinstance(basestring),
#                    input_to_email,
                    cleanup_line,
                    ),
                capacity = pipe(
                    test_isinstance(basestring),
                    test_in([u'private', u'public', u'organization']),
                    ),
                creator_user_id = ckan_json_to_id,  # Set by ckanext-harvest
                id = ckan_json_to_id,
                license_id = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                maintainer = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                maintainer_email = pipe(
                    test_isinstance(basestring),
#                    input_to_email,
                    cleanup_line,
                    ),
                metadata_modified = ckan_json_to_iso8601_date_str,
                name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
                    ),
                notes = pipe(
                    test_isinstance(basestring),
                    cleanup_text,
                    ),
                owner_org = ckan_json_to_id,
                private = test_isinstance(bool),
                revision_id = ckan_json_to_id,
                state = ckan_json_to_state,
                title = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
                    ),
                type = pipe(
                    test_isinstance(basestring),
                    translate({u'None': None}),
                    test_in([u'dataset', u'harvest']),
                    default(u'dataset'),
                    ),
                url = pipe(
                    test_isinstance(basestring),
                    make_input_to_url(add_prefix = u'http://', full = True),
                    ),
                version = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                ),
            drop_none_values = drop_none_values,
            keep_value_order = keep_value_order,
            skip_missing_items = skip_missing_items,
            ),
        )


def make_ckan_json_to_embedded_user(drop_none_values = False, keep_value_order = False, skip_missing_items = False):
    return pipe(
        test_isinstance(dict),
        struct(
            dict(
                about = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                activity_streams_email_notifications = test_isinstance(bool),
                apikey = test_isinstance(basestring),
                capacity = pipe(
                    test_isinstance(basestring),
                    test_in([u'admin', u'editor', u'member']),
                    not_none,
                    ),
                created = ckan_json_to_iso8601_datetime_str,
                display_name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                email = pipe(
                    test_isinstance(basestring),
#                    input_to_email,
                    cleanup_line,
                    ),
                email_hash = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                fullname = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                id = ckan_json_to_id,
                name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
                    ),
                number_administered_packages = pipe(
                    test_isinstance(int),
                    test_greater_or_equal(0),
                    ),
                number_of_edits = pipe(
                    test_isinstance(int),
                    test_greater_or_equal(0),
                    ),
                openid = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    test_equals(u'TODO'),
                    ),
                reset_key = test_isinstance(basestring),
                sysadmin = test_isinstance(bool),
                ),
            drop_none_values = drop_none_values,
            keep_value_order = keep_value_order,
            skip_missing_items = skip_missing_items,
            ),
        )


def make_ckan_json_to_group(drop_none_values = False, keep_value_order = False, skip_missing_items = False):
    return pipe(
        test_isinstance(dict),
        remove_extras,
        struct(
            dict(
                approval_status = pipe(
                    ckan_json_to_approval_status,
                    not_none,
                    ),
                capacity = pipe(
                    test_isinstance(basestring),
                    test_in([u'private', u'public']),
                    ),
                created = ckan_json_to_iso8601_datetime_str,
                description = pipe(
                    test_isinstance(basestring),
                    cleanup_text,
                    ),
                display_name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                extras = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            test_isinstance(dict),
                            struct(
                                dict(
                                    __extras = pipe(
                                        test_isinstance(dict),
                                        struct(
                                            dict(
                                                group_id = pipe(
                                                    ckan_json_to_id,
                                                    not_none,
                                                    ),
                                                revision_id = pipe(
                                                    ckan_json_to_id,
                                                    not_none,
                                                    ),
                                                ),
                                            ),
                                        ),
                                    group_id = ckan_json_to_id,
                                    id = ckan_json_to_id,
                                    key = pipe(
                                        test_isinstance(basestring),
                                        cleanup_line,
                                        not_none,
                                        ),
                                    revision_id = ckan_json_to_id,
                                    state = ckan_json_to_state,
                                    value = pipe(
                                        test_isinstance(basestring),
                                        cleanup_line,
                                        ),
                                    ),
                                ),
                            not_none,
                            ),
                        ),
                    empty_to_none,
                    ),
                groups = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            make_ckan_json_to_embedded_group(drop_none_values = drop_none_values,
                                keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                            not_none,
                            ),
                        ),
                    empty_to_none,
                    ),
                id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                image_url = pipe(
                    test_isinstance(basestring),
                    make_input_to_url(add_prefix = u'http://', full = True),
                    ),
                name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
                    ),
                packages = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            make_ckan_json_to_embeded_package(drop_none_values = drop_none_values,
                                keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                            not_none,
                            ),
                        ),
                    empty_to_none,
                    ),
                revision_id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                state = ckan_json_to_state,
                tags = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            make_ckan_json_to_tag(drop_none_values = drop_none_values,
                                keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                            not_none,
                            ),
                        ),
                    empty_to_none,
                    ),
                title = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
                    ),
                type = pipe(
                    ckan_json_to_group_type,
                    not_none,
                    ),
                users = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            make_ckan_json_to_embedded_user(drop_none_values = drop_none_values,
                                keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                            not_none,
                            ),
                        ),
                    not_none,
                    empty_to_none,
                    ),
                ),
            drop_none_values = drop_none_values,
            keep_value_order = keep_value_order,
            skip_missing_items = skip_missing_items,
            ),
        )


def make_ckan_json_to_organization(drop_none_values = False, keep_value_order = False, skip_missing_items = False):
    return pipe(
        test_isinstance(dict),
        remove_extras,
        struct(
            dict(
                approval_status = pipe(
                    ckan_json_to_approval_status,
                    not_none,
                    ),
                created = pipe(
                    ckan_json_to_iso8601_date_str,
                    not_none,
                    ),
                description = pipe(
                    test_isinstance(basestring),
                    cleanup_text,
                    ),
                display_name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                extras = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            test_isinstance(dict),
                            struct(
                                dict(
                                    group_id = pipe(
                                        ckan_json_to_id,
                                        not_none,
                                        ),
                                    id = pipe(
                                        ckan_json_to_id,
                                        not_none,
                                        ),
                                    key = pipe(
                                        test_isinstance(basestring),
                                        cleanup_line,
                                        not_none,
                                        ),
                                    revision_id = pipe(
                                        ckan_json_to_id,
                                        not_none,
                                        ),
                                    state = pipe(
                                        ckan_json_to_state,
                                        not_none,
                                        ),
                                    value = pipe(
                                        test_isinstance(basestring),
                                        cleanup_line,
                                        ),
                                    ),
                                ),
                            not_none,
                            ),
                        ),
                    not_none,
                    empty_to_none,
                    ),
                groups = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            make_ckan_json_to_embedded_group(drop_none_values = drop_none_values,
                                keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                            not_none,
                            ),
                        ),
                    empty_to_none,
                    ),
                id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                image_url = pipe(
                    test_isinstance(basestring),
                    make_input_to_url(add_prefix = u'http://', full = True),
                    ),
                is_organization = pipe(
                    test_isinstance(bool),
                    not_none,
                    ),
                name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
                    ),
                num_followers = pipe(
                    test_isinstance(int),
                    test_greater_or_equal(0),
                    not_none,
                    ),
                package_count = pipe(
                    test_isinstance(int),
                    test_greater_or_equal(0),
                    not_none,
                    ),
                packages = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            make_ckan_json_to_embedded_package(drop_none_values = drop_none_values,
                                keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                            not_none,
                            ),
                        ),
                    empty_to_none,
                    ),
                revision_id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                revision_timestamp = ckan_json_to_iso8601_datetime_str,
                state = pipe(
                    ckan_json_to_state,
                    not_none,
                    ),
                tags = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            make_ckan_json_to_tag(drop_none_values = drop_none_values,
                                keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                            not_none,
                            ),
                        ),
                    not_none,
                    empty_to_none,
                    ),
                title = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
                    ),
                type = pipe(
                    test_isinstance(basestring),
                    test_equals(u'organization'),
                    ),
                users = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            make_ckan_json_to_embedded_user(drop_none_values = drop_none_values,
                                keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                            not_none,
                            ),
                        ),
                    not_none,
                    empty_to_none,
                    ),
                ),
            drop_none_values = drop_none_values,
            keep_value_order = keep_value_order,
            skip_missing_items = skip_missing_items,
            ),
        )


def make_ckan_json_to_package(drop_none_values = False, keep_value_order = False, skip_missing_items = False):
    return pipe(
        test_isinstance(dict),
        remove_extras,
        struct(
            dict(
                author = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                author_email = pipe(
                    test_isinstance(basestring),
#                    input_to_email,
                    cleanup_line,
                    ),
                capacity = pipe(
                    test_isinstance(basestring),
                    test_in([u'private', u'public']),
                    ),
                creator_user_id = ckan_json_to_id,  # Set by ckanext-harvest
                extras = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            test_isinstance(dict),
                            struct(
                                dict(
                                    __extras = pipe(
                                        test_isinstance(dict),
                                        struct(
                                            dict(
                                                package_id = pipe(
                                                    ckan_json_to_id,
                                                    not_none,
                                                    ),
                                                revision_id = pipe(
                                                    ckan_json_to_id,
                                                    not_none,
                                                    ),
                                                ),
                                            ),
                                        ),
                                    deleted = test_equals(True),
                                    id = ckan_json_to_id,
                                    key = pipe(
                                        test_isinstance(basestring),
                                        cleanup_line,
                                        not_none,
                                        ),
                                    package_id = ckan_json_to_id,
                                    revision_id = ckan_json_to_id,
                                    revision_timestamp = ckan_json_to_iso8601_datetime_str,
                                    state = ckan_json_to_state,
                                    value = pipe(
                                        test_isinstance(basestring),
                                        cleanup_line,
                                        ),
                                    ),
                                ),
                            not_none,
                            ),
                        ),
                    empty_to_none,
                    ),
                groups = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            make_ckan_json_to_embedded_group(drop_none_values = drop_none_values,
                                keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                            not_none,
                            ),
                        ),
                    empty_to_none,
                    ),
                id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                isopen = pipe(
                    test_isinstance(bool),
                    not_none,
                    ),
                license_id = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                license_title = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                license_url = pipe(
                    test_isinstance(basestring),
                    make_input_to_url(full = True),
                    ),
                maintainer = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                maintainer_email = pipe(
                    test_isinstance(basestring),
#                    input_to_email,
                    cleanup_line,
                    ),
                metadata_created = pipe(
                    ckan_json_to_iso8601_date_str,
                    not_none,
                    ),
                metadata_modified = pipe(
                    ckan_json_to_iso8601_date_str,
                    not_none,
                    ),
                name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
                    ),
                notes = pipe(
                    test_isinstance(basestring),
                    cleanup_text,
                    ),
                num_resources = pipe(
                    test_isinstance(int),
                    test_greater_or_equal(0),
                    ),
                num_tags = pipe(
                    test_isinstance(int),
                    test_greater_or_equal(0),
                    ),
                organization = make_ckan_json_to_package_organization(drop_none_values = drop_none_values,
                    keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                owner_org = ckan_json_to_id,
                private = test_isinstance(bool),
                relationships_as_object = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            test_equals('TODO'),
                            not_none,
                            ),
                        ),
                    empty_to_none,
                    ),
                relationships_as_subject = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            test_equals('TODO'),
                            not_none,
                            ),
                        ),
                    empty_to_none,
                    ),
                resources = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            make_ckan_json_to_resource(drop_none_values = drop_none_values,
                                keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                            not_none,
                            ),
                        ),
                    empty_to_none,
                    ),
                revision_id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                revision_timestamp = pipe(
                    ckan_json_to_iso8601_datetime_str,
                    not_none,
                    ),
                state = pipe(
                    ckan_json_to_state,
                    not_none,
                    ),
                supplier = make_ckan_json_to_package_organization(drop_none_values = drop_none_values,
                    keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                supplier_id = ckan_json_to_id,
                tags = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            make_ckan_json_to_tag(drop_none_values = drop_none_values,
                                keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                            not_none,
                            ),
                        ),
                    empty_to_none,
                    ),
#                temporal_coverage_from = ckan_json_to_iso8601_date_str,
                temporal_coverage_from = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
#                temporal_coverage_to = ckan_json_to_iso8601_date_str,
                temporal_coverage_to = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                territorial_coverage = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                territorial_coverage_granularity = pipe(
                    test_isinstance(basestring),
                    cleanup_line,  # TODO
                    ),
                title = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
                    ),
                tracking_summary = make_ckan_json_to_tracking_summary(drop_none_values = drop_none_values,
                    keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                type = pipe(
                    test_isinstance(basestring),
                    translate({u'None': None}),
                    test_in([u'dataset', u'harvest']),
                    default(u'dataset'),
                    ),
                url = pipe(
                    test_isinstance(basestring),
                    make_input_to_url(add_prefix = u'http://', full = True),
                    ),
                version = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                ),
            drop_none_values = drop_none_values,
            keep_value_order = keep_value_order,
            skip_missing_items = skip_missing_items,
            ),
        )


def make_ckan_json_to_package_organization(drop_none_values = False, keep_value_order = False,
        skip_missing_items = False):
    return pipe(
        test_isinstance(dict),
        struct(
            dict(
                approval_status = pipe(
                    ckan_json_to_approval_status,
                    not_none,
                    ),
                created = pipe(
                    ckan_json_to_iso8601_date_str,
                    not_none,
                    ),
                description = pipe(
                    test_isinstance(basestring),
                    cleanup_text,
                    ),
                id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                image_url = pipe(
                    test_isinstance(basestring),
                    make_input_to_url(add_prefix = u'http://', full = True),
                    ),
                is_organization = pipe(
                    test_isinstance(bool),
                    not_none,
                    ),
                name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
                    ),
                revision_id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                revision_timestamp = pipe(
                    ckan_json_to_iso8601_datetime_str,
                    not_none,
                    ),
                state = pipe(
                    ckan_json_to_state,
                    not_none,
                    ),
                title = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
                    ),
                type = pipe(
                    test_isinstance(basestring),
                    test_equals(u'organization'),
                    ),
                ),
            drop_none_values = drop_none_values,
            keep_value_order = keep_value_order,
            skip_missing_items = skip_missing_items,
            ),
        )


def make_ckan_json_to_resource(drop_none_values = False, keep_value_order = False, skip_missing_items = False):
    return pipe(
        test_isinstance(dict),
        struct(
            dict(
                cache_last_updated = ckan_json_to_iso8601_datetime_str,
                cache_url = pipe(
                    test_isinstance(basestring),
                    make_input_to_url(full = True),
                    ),
                cache_url_updated = pipe(
                    translate({
                        u'NaN-NaN-NaNTNaN:NaN:NaN': None,
                        u'None': None,
                        }),
                    ckan_json_to_iso8601_datetime_str,
                    ),
                created = pipe(
                    ckan_json_to_iso8601_date_str,
                    not_none,
                    ),
                description = pipe(
                    test_isinstance(basestring),
                    cleanup_text,
                    ),
                format = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                hash = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                last_modified = ckan_json_to_iso8601_date_str,
                mimetype = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                mimetype_inner = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    # Some resources have no name.
                    ),
                owner = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                position = pipe(
                    test_isinstance(int),
                    test_greater_or_equal(0),
                    ),
                resource_group_id = ckan_json_to_id,
                resource_type = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    test_in([u'api', 'documentation', 'file', 'file.upload', 'image', 'metadata', 'visualization']),
                    ),
                revision_id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                revision_timestamp = ckan_json_to_iso8601_datetime_str,
                size = pipe(
                    condition(
                        test_isinstance(basestring),
                        input_to_int,
                        test_isinstance(int),
                        ),
                    test_greater_or_equal(0),
                    ),
                state = ckan_json_to_state,
                tracking_summary = make_ckan_json_to_tracking_summary(drop_none_values = drop_none_values,
                    keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                url = pipe(
                    test_isinstance(basestring),
                    make_input_to_url(add_prefix = u'http://', full = True),
                    ),
                url_error = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                url_type = pipe(
                    test_isinstance(basestring),
                    test_in(['TODO']),
                    ),
                webstore_last_updated = ckan_json_to_iso8601_date_str,
                webstore_url = pipe(
                    # https://github.com/okfn/ckan/issues/931: Remove webstore_url from resources.
                    test_isinstance(basestring),
                    cleanup_line,  # May be ''.
                    first_match(
                        test_in([u'active']),
                        make_input_to_url(full = True),
                        ),
                    ),
                ),
            drop_none_values = drop_none_values,
            keep_value_order = keep_value_order,
            skip_missing_items = skip_missing_items,
            ),
        )


def make_ckan_json_to_tag(drop_none_values = False, keep_value_order = False, skip_missing_items = False):
    return pipe(
        test_isinstance(dict),
        struct(
            dict(
                display_name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                revision_timestamp = pipe(
                    ckan_json_to_iso8601_datetime_str,
                    not_none,
                    ),
                state = pipe(
                    ckan_json_to_state,
                    not_none,
                    ),
                vocabulary_id = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                ),
            drop_none_values = drop_none_values,
            keep_value_order = keep_value_order,
            skip_missing_items = skip_missing_items,
            ),
        )


def make_ckan_json_to_tracking_summary(drop_none_values = False, keep_value_order = False, skip_missing_items = False):
    return pipe(
        test_isinstance(dict),
        struct(
            dict(
                recent = pipe(
                    test_isinstance(int),
                    test_greater_or_equal(0),
                    not_none,
                    ),
                total = pipe(
                    test_isinstance(int),
                    test_greater_or_equal(0),
                    not_none,
                    ),
                ),
            drop_none_values = drop_none_values,
            keep_value_order = keep_value_order,
            skip_missing_items = skip_missing_items,
            ),
        )


def make_ckan_json_to_user(drop_none_values = False, keep_value_order = False, skip_missing_items = False):
    return pipe(
        test_isinstance(dict),
        struct(
            dict(
                about = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                activity_streams_email_notifications = pipe(
                    test_isinstance(bool),
                    not_none,
                    ),
                apikey = pipe(
                    test_isinstance(basestring),
                    not_none,
                    ),
                capacity = pipe(
                    test_isinstance(basestring),
                    test_in([u'admin', u'editor', u'member']),
                    not_none,
                    ),
                created = pipe(
                    ckan_json_to_iso8601_datetime_str,
                    not_none,
                    ),
                display_name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                email = pipe(
                    test_isinstance(basestring),
#                    input_to_email,
                    cleanup_line,
                    not_none,
                    ),
                email_hash = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                fullname = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    ),
                id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                name = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
                    ),
                number_administered_packages = pipe(
                    test_isinstance(int),
                    test_greater_or_equal(0),
                    not_none,
                    ),
                number_of_edits = pipe(
                    test_isinstance(int),
                    test_greater_or_equal(0),
                    not_none,
                    ),
                openid = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    test_equals(u'TODO'),
                    ),
                reset_key = test_isinstance(basestring),
                sysadmin = pipe(
                    test_isinstance(bool),
                    not_none,
                    ),
                ),
            drop_none_values = drop_none_values,
            keep_value_order = keep_value_order,
            skip_missing_items = skip_missing_items,
            ),
        )


def remove_extras(value, state = None):
    if value is None:
        return value, None
    clean_value = value.copy()  # A non-deep copy is sufficient.
    for extra in (value.get('extras') or []):
        key = extra.get('key')
        if not isinstance(key, basestring):
            continue
        if key in value and value[key] == extra.get('value'):
            del clean_value[key]
    return clean_value, None
