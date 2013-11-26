#
# Copyright 2012-2013 Ghent University
#
# This file is part of vsc-base,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# http://github.com/hpcugent/vsc-base
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
# along with vsc-base. If not, see <http://www.gnu.org/licenses/>.
#
"""
Mocking module for vsc-zk

@author: Kenneth Waegemam (Ghent University)
"""
class KazooClient(object):

    BASE_ZNODE = '/admin'

    def __init__(self, hosts, auth_data=None, default_acl=None):
        pass

    def start(self):
        pass

class Party(object):
    def __init__(self, dummy1, dummy2, dummy3, **kwargs):
        pass

    def join(self):
        pass

class LockingQueue(object):
    def __init__(self, thingy, name, **kwargs):
        pass
    def put(self, something):
        pass
