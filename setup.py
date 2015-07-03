#!/usr/bin/env python
# -*- coding: latin-1 -*-
#
# Copyright 2013-2013 Ghent University
#
# This file is part of vsc-zk,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# http://github.com/hpcugent/vsc-zk
#
# vsc-zk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Library General Public License as
# published by the Free Software Foundation, either version 2 of
# the License, or (at your option) any later version.
#
# vsc-zk is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public License
# along with vsc-zk. If not, see <http://www.gnu.org/licenses/>.
#
"""
vsc-zk base distribution setup.py

@author: Stijn De Weirdt (Ghent University)
"""
import vsc.install.shared_setup as shared_setup
from vsc.install.shared_setup import ag, kw, sdw


def remove_bdist_rpm_source_file():
    """List of files to remove from the (source) RPM."""
    return ['lib/vsc/__init__.py']


shared_setup.remove_extra_bdist_rpm_files = remove_bdist_rpm_source_file
shared_setup.SHARED_TARGET.update({
    'url': 'https://github.com/hpcugent/vsc-zk',
    'download_url': 'https://github.com/hpcugent/vsc-zk'
})

PACKAGE = {
    'name': 'vsc-zk',
    'version': '0.7.2',
    'author': [sdw],
    'maintainer': [sdw, kw],
    'packages': ['vsc', 'vsc.zk', 'vsc.zk.rsync'],
    'namespace_packages': ['vsc'],
    'scripts': [
                'bin/zkrsync.py',
                'bin/zkinitree.py',
                ],
    'install_requires': [
        'vsc-base >= 1.6.7',
        'kazoo >= 1.3',
    ],
}


if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
