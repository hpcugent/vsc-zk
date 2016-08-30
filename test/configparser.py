#
# Copyright 2012-2016 Ghent University
#
# This file is part of vsc-zk,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-zk
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
Unit tests for configparser

@author: Kenneth Waegeman (Ghent University)
"""

import vsc.zk.configparser as cp

from kazoo.security import make_digest_acl, ACL, Id
from vsc.install.testing import TestCase

class ConfigParserTest(TestCase):
    """Tests for depthwalk"""

    def setUp(self):
        """ Uses a test output of simple_option configparser """
        self.cfgremainder = {'user4': {'passwd': 'bla'}, 'user5': {'passwd': 'bla'}, 'user2': {'passwd': 'bar'},
                             'user3': {'passwd': 'bla'}, 'user1': {'passwd': 'xyz'}, 'rsync': {'passwd': 'w00f'},
                             'sys': {'passwd': 'sis'}, 'kwaegema': {'passwd': 'w00f'}, '/admin/tools': {'all': 'sys'},
                             '/admin/rsync': {'rwcd': 'rsync,kwaegema', 'all': ''},
                             '/admin': {'rwcd': 'user1', 'c': 'user3', 'rw': 'user4,user3', 'all': '',
                                        'cd': 'user2', 'r': 'user5'},
                             '/admin/user1': {'all': 'user1', 'r': 'user2,user3', 'rw': 'user4'},
                             'root': {'passwd': 'admin', 'path': '/'}}

        self.rootacl = make_digest_acl('root', 'admin', all=True)
        super(ConfigParserTest, self).setUp()

    def test_rootinfo(self):
        """Checks if root info got parsed correctly"""
        self.assertEqual(cp.get_rootinfo(self.cfgremainder), ('admin', '/'))

    def test_parse_zkconfig(self):
        """ Checks if configuration file got parsed correctly"""
        res = ({'/admin': {'rwcd': ['user1'], 'c': ['user3'], 'rw': ['user4', 'user3'], 'r': ['user5'], 'cd': ['user2']},
                '/admin/user1': {'all': ['user1'], 'r': ['user2', 'user3'], 'rw': ['user4']},
                '/admin/tools': {'all': ['sys']},
                '/admin/rsync': {'rwcd': ['rsync', 'kwaegema']}},
               {'user4': {'passwd': ['bla']}, 'user5': {'passwd': ['bla']}, 'user2': {'passwd': ['bar']},
                'user3': {'passwd': ['bla']}, 'user1': {'passwd': ['xyz']}, 'rsync': {'passwd': ['w00f']},
                'sys': {'passwd': ['sis']}, 'kwaegema': {'passwd': ['w00f']}})
        cp.get_rootinfo(self.cfgremainder)
        self.assertEqual(cp.parse_zkconfig(self.cfgremainder), res)
