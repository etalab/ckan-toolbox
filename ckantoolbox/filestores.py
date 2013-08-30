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


"""Toolbox to use CKAN FileStore API"""


import datetime
import itertools
import json
import mimetools
import mimetypes
import os
import urllib2
import urlparse

from biryani1 import strings


class MultiPartForm(object):
    """Accumulate the data to be used when posting a form."""

    def __init__(self):
        self.form_fields = []
        self.files = []
        self.boundary = mimetools.choose_boundary()

    def __str__(self):
        """Return a string representing the form data, including attached files."""
        # Build a list of lists, each containing "lines" of the request. Each part is separated by a boundary string.
        # Once the list is built, return a string where each line is separated by '\r\n'.
        parts = []
        part_boundary = '--' + self.boundary

        # Add the form fields.
        parts.extend(
            [
                part_boundary,
                'Content-Disposition: form-data; name="%s"' % name,
                '',
                value,
                ]
            for name, value in self.form_fields
            )

        # Add the files to upload.
        parts.extend(
            [
                part_boundary,
                'Content-Disposition: file; name="%s"; filename="%s"' % (field_name, filename),
                'Content-Type: %s' % content_type,
                '',
                body,
                ]
            for field_name, filename, content_type, body in self.files
            )

        # Flatten the list and add closing boundary marker,
        # then return CR+LF separated data
        flattened = list(itertools.chain(*parts))
        flattened.append('--' + self.boundary + '--')
        flattened.append('')
        return '\r\n'.join(flattened)

    def add_field(self, name, value):
        """Add a simple field to the form data."""
        self.form_fields.append((str(name), strings.deep_encode(value)))

    def add_file_bytes(self, fieldname, filename, file_bytes, mimetype = None):
        """Add a file to be uploaded."""
        if mimetype is None:
            mimetype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        self.files.append((str(fieldname), strings.deep_encode(filename), str(mimetype), file_bytes))

    @property
    def content_type(self):
        return 'multipart/form-data; boundary=%s' % self.boundary


def upload_file(site_url, filename, file_data, headers):
    assert 'Authorization' in headers, headers

    # See ckan/public/application.js:makeUploadKey for why the file_key is derived this way.
    timestamp = datetime.datetime.now().isoformat().replace(':', '').split('.')[0]
    normalized_name = os.path.basename(filename).replace(' ', '-')
    file_key = u'{}/{}'.format(timestamp, normalized_name)
    request = urllib2.Request(urlparse.urljoin(site_url, u'/api/storage/auth/form/{}'.format(file_key)),
        headers = headers)
    response = urllib2.urlopen(request)
    file_upload_fields = json.loads(response.read())

    form = MultiPartForm()
    for field in file_upload_fields['fields']:
        form.add_field(field['name'], unicode(field['value']).encode('utf-8'))
    form.add_file_bytes('file', file_key.encode('utf-8'), file_data)
    form_bytes = str(form)
    form_headers = headers.copy()
    form_headers.update({
        'Content-Length': len(form_bytes),
        'Content-Type': form.content_type,
        })
    request = urllib2.Request(unicode(urlparse.urljoin(site_url, file_upload_fields['action'])).encode('utf-8'),
        headers = form_headers)
    request.add_data(form_bytes)
    response = urllib2.urlopen(request)
    response_text = response.read()

    request = urllib2.Request(urlparse.urljoin(site_url, u'/api/storage/metadata/{}'.format(file_key)),
        headers = headers)
    response = urllib2.urlopen(request)
    response_text = response.read()
    file_metadata = json.loads(response_text)
    return file_metadata
