#
# Copyright 2012-2013 Ghent University
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
Mocking module for Kazoo/Zookeeper classes 

@author: Kenneth Waegeman (Ghent University)
"""
class KazooClient(object):

    BASE_ZNODE = '/admin'


    def __init__(self, hosts, auth_data=None, default_acl=None):
        self.objs = {}
        self.whoami = 'test'

    def start(self):
        pass

    def ensure_path(self, path):
        pass

    def set(self, obj, value):
        self.objs[obj] = value;

    def create(self, obj, value, **kw):
        return self.set(obj, value)

    def get(self, obj):
        return (self.objs[obj], 'dummy')

    def exists(self, obj):
        return obj in self.objs

    def Lock(self, path, id):
        return Lock(self)
        pass

    def print_objs(self):
        print  self.objs

class Lock(object):
    def __init__(self, dummy1):
        pass
    def __exit__(self, bl, er, erb):
        pass
    def __enter__(self):
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
    def consume(self):
        pass

class Counter(object):
    def __init__(self, client, path, default=0):

        self.default = default
        self.default_type = type(default)
        self.value = default


    def _change(self, value):
        if not isinstance(value, self.default_type):
            raise TypeError('invalid type for value change')
        self.value = self.value + value
        return self


    def __add__(self, value):
        """Add value to counter."""
        return self._change(value)

    def __sub__(self, value):
        """Subtract value from counter."""
        return self._change(-value)
