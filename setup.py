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


"""Various modules that handle CKAN API and data"""


try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages


classifiers = """\
Development Status :: 2 - Pre-Alpha
Environment :: Web Environment
Intended Audience :: Developers
License :: OSI Approved :: GNU Affero General Public License v3
Operating System :: OS Independent
Programming Language :: Python
Topic :: Software Development :: Libraries :: Python Modules
"""

doc_lines = __doc__.split('\n')


setup(
    name = 'CKAN-Toolbox',
    version = '0.1dev',

    author = 'Emmanuel Raviart',
    author_email = 'emmanuel@raviart.com',
    classifiers = [classifier for classifier in classifiers.split('\n') if classifier],
    description = doc_lines[0],
    keywords = 'api ckan conversion data dataset python validation',
    license = 'http://www.fsf.org/licensing/licenses/agpl-3.0.html',
    long_description = '\n'.join(doc_lines[2:]),
    url = 'http://github.com/etalab/ckan-toolbox',

    packages = find_packages(exclude = ['ez_setup']),
    zip_safe = False,
    )
