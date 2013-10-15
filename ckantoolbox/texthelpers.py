#! /usr/bin/env python
# -*- coding: utf-8 -*-


# CKAN-Toolbox -- Various modules that handle CKAN API and data
# By: Emmanuel Raviart <emmanuel@raviart.com>
#
# Copyright (C) 2013 Etalab
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


"""Helpers to handle strings"""


import re

from biryani1 import strings


tag_char_re = re.compile(ur'[- \w]', re.UNICODE)


def namify(text, encoding = 'utf-8'):
    """Convert a string to a CKAN name."""
    if text is None:
        return None
    if isinstance(text, str):
        text = text.decode(encoding)
    assert isinstance(text, unicode), str((text,))
    simplified = u''.join(namify_char(unicode_char) for unicode_char in text)
    # CKAN accepts names with duplicate "-" or "_" and/or ending with "-" or "_".
    #while u'--' in simplified:
    #    simplified = simplified.replace(u'--', u'-')
    #while u'__' in simplified:
    #    simplified = simplified.replace(u'__', u'_')
    #simplified = simplified.strip(u'-_')
    return simplified


def namify_char(unicode_char):
    """Convert an unicode character to a subset of lowercase ASCII characters or an empty string.

    The result can be composed of several characters (for example, 'œ' becomes 'oe').
    """
    chars = strings.unicode_char_to_ascii(unicode_char)
    if chars:
        chars = chars.lower()
        split_chars = []
        for char in chars:
            if char not in '-_0123456789abcdefghijklmnopqrstuvwxyz':
                char = '-'
            split_chars.append(char)
        chars = ''.join(split_chars)
    return chars


def tag_namify(text, encoding = 'utf-8'):
    """Convert a string to a CKAN tag name."""
    if text is None:
        return None
    if isinstance(text, str):
        text = text.decode(encoding)
    assert isinstance(text, unicode), str((text,))
    simplified = u''.join(tag_namify_char(unicode_char) for unicode_char in text)
    # CKAN accepts tag names with duplicate "-" or "_" and/or ending with "-" or "_".
    #while u'--' in simplified:
    #    simplified = simplified.replace(u'--', u'-')
    #while u'__' in simplified:
    #    simplified = simplified.replace(u'__', u'_')
    #simplified = simplified.strip(u'-_')
    return simplified


def tag_namify_char(unicode_char):
    """Convert an unicode character to a subset of lowercase characters or an empty string."""
    unicode_char = unicode_char.lower()
    if tag_char_re.match(unicode_char) is None:
        unicode_char = u'-'
    return unicode_char
