#! /usr/bin/env python
# -*- coding: utf-8 -*-


# CKAN-Toolbox -- Various modules that handle CKAN API and data
# By: Emmanuel Raviart <emmanuel@raviart.com>
#
# Copyright (C) 2013 Emmanuel Raviart
# http://gitorious.org/etalab/ckan-toolbox
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
    empty_to_none,
    input_to_email,
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


ckan_json_to_package_list = pipe(
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
    test_in([u'active']),
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


def make_ckan_json_to_group(drop_none_values = False, keep_value_order = False, skip_missing_items = False):
    return pipe(
        test_isinstance(dict),
        struct(
            dict(
                description = pipe(
                    test_isinstance(basestring),
                    cleanup_text,
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
                title = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
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
        struct(
            dict(
                approval_status = pipe(
                    test_isinstance(basestring),
                    test_in([u'approved']),
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
                            test_equals('TODO'),
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
                            test_equals('TODO'),
                            not_none,
                            ),
                        ),
                    not_none,
                    empty_to_none,
                    ),
                id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                image_url = pipe(
                    test_isinstance(basestring),
                    make_input_to_url(full = True),
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
                            make_ckan_json_to_organization_package(drop_none_values = drop_none_values,
                                keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                            not_none,
                            ),
                        ),
                    not_none,
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
                            test_equals('TODO'),
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


def make_ckan_json_to_organization_package(drop_none_values = False, keep_value_order = False,
        skip_missing_items = False):
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
                    input_to_email,
                    ),
                capacity = pipe(
                    test_isinstance(basestring),
                    test_equals(u'organization'),
                    not_none,
                    ),
                id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
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
                    input_to_email,
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
                owner_org = ckan_json_to_id,
                private = pipe(
                    test_isinstance(bool),
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
                title = pipe(
                    test_isinstance(basestring),
                    cleanup_line,
                    not_none,
                    ),
                type = pipe(
                    test_isinstance(basestring),
                    test_in([u'dataset']),
                    not_none,
                    ),
                url = pipe(
                    test_isinstance(basestring),
                    make_input_to_url(full = True),
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


def make_ckan_json_to_package(drop_none_values = False, keep_value_order = False, skip_missing_items = False):
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
                    input_to_email,
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
                                        not_none,
                                        ),
                                    deleted = test_equals(True),
                                    key = pipe(
                                        test_isinstance(basestring),
                                        cleanup_line,
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
                    empty_to_none,
                    ),
                groups = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            make_ckan_json_to_group(drop_none_values = drop_none_values,
                                keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                            not_none,
                            ),
                        ),
                    not_none,
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
                    input_to_email,
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
                    not_none,
                    ),
                num_tags = pipe(
                    test_isinstance(int),
                    test_greater_or_equal(0),
                    not_none,
                    ),
                organization = make_ckan_json_to_package_organization(drop_none_values = drop_none_values,
                    keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                owner_org = ckan_json_to_id,
                private = pipe(
                    test_isinstance(bool),
                    not_none,
                    ),
                relationships_as_object = pipe(
                    test_isinstance(list),
                    uniform_sequence(
                        pipe(
                            test_equals('TODO'),
                            not_none,
                            ),
                        ),
                    not_none,
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
                    not_none,
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
                    not_none,
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
                tracking_summary = pipe(
                    make_ckan_json_to_tracking_summary(drop_none_values = drop_none_values,
                        keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                    not_none,
                    ),
                type = pipe(
                    test_isinstance(basestring),
                    test_in([u'dataset']),
                    not_none,
                    ),
                url = pipe(
                    test_isinstance(basestring),
                    make_input_to_url(full = True),
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
                    test_isinstance(basestring),
                    test_in([u'approved']),
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
                    make_input_to_url(full = True),
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
                    translate({u'None': None}),
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
                resource_group_id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                resource_type = pipe(
                    test_isinstance(basestring),
                    test_in([u'api', 'file', 'file.upload']),
                    ),
                revision_id = pipe(
                    ckan_json_to_id,
                    not_none,
                    ),
                revision_timestamp = ckan_json_to_iso8601_datetime_str,
                size = pipe(
                    test_isinstance(int),
                    test_greater_or_equal(0),
                    ),
                state = pipe(
                    ckan_json_to_state,
                    not_none,
                    ),
                tracking_summary = pipe(
                    make_ckan_json_to_tracking_summary(drop_none_values = drop_none_values,
                        keep_value_order = keep_value_order, skip_missing_items = skip_missing_items),
                    not_none,
                    ),
                url = pipe(
                    test_isinstance(basestring),
                    make_input_to_url(full = True),
                    ),
                webstore_last_updated = ckan_json_to_iso8601_date_str,
                webstore_url = pipe(
                    test_isinstance(basestring),
                    make_input_to_url(full = True),
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
