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
# All rights reserved.
#
#
"""
vsc-zk base distribution setup.py

@author: Stijn De Weirdt (Ghent University)
"""
import vsc.install.shared_setup as shared_setup
from vsc.install.shared_setup import ag, sdw


def remove_bdist_rpm_source_file():
    """List of files to remove from the (source) RPM."""
    return ['lib/vsc/__init__.py']


shared_setup.remove_extra_bdist_rpm_files = remove_bdist_rpm_source_file
shared_setup.SHARED_TARGET.update({
    'url': 'https://github.ugent.be/hpcugent/vsc-zk',
    'download_url': 'https://github.ugent.be/hpcugent/vsc-zk'
})

PACKAGE = {
    'name': 'vsc-zk',
    'version': '0.5.0',
    'author': [sdw],
    'maintainer': [sdw, ag],
    'packages': ['vsc', 'vsc.zk', 'vsc.zk.rsync'],
    'namespace_packages': ['vsc'],
    'scripts': [
                'bin/zkrsync.py',
                'bin/zkinitree.py',
                ],
    'install_requires': [
        'vsc-base >= 1.2',
        'kazoo >= 1.3',
    ],
    'provides': ['python-vsc-zk = 0.5.0'],
}


if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
