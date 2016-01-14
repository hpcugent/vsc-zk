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
Unit tests for VscKazooClient and childs

@author: Kenneth Waegeman (Ghent University)
"""
import sys
import time

from kazoo.client import KazooClient
from kazoo.recipe.party import Party
from kazoo.recipe.queue import LockingQueue

sys.modules['kazoo.client'] = __import__('mock')
sys.modules['kazoo.recipe.queue'] = __import__('mock')
sys.modules['kazoo.recipe.party'] = __import__('mock')

from unittest import TestCase, TestLoader
from vsc.zk.base import VscKazooClient, RunWatchLoopLog
from vsc.zk.rsync.controller import RsyncController
from vsc.zk.rsync.destination import RsyncDestination
from vsc.zk.rsync.source import RsyncSource

class zkClientTest(TestCase):

    def setUp(self):
        pass

    def test_znode_path(self):
        """ Test the correct creation of a zookeeper path """
        zkclient = VscKazooClient('mocked')
        self.assertEqual(zkclient.znode_path('test'), '/admin/test')
        self.assertEqual(zkclient.znode_path('/admin/test'), '/admin/test')
        self.assertRaises(Exception, zkclient.znode_path, '/other/test')

    def test_set_is_ready(self):
        """ Test the ready function """
        zkclient = VscKazooClient('mocked')
        self.assertFalse(zkclient.is_ready())
        zkclient.set_ready()
        self.assertTrue(zkclient.is_ready())

    def test_rsync_params(self):
        """ Test some parameters of Source, Destination and Controller classes """
        # Path check failed test
        self.assertRaises(Exception, RsyncController, 'dummy', rsyncpath='/non/existing/path/dummy')
        # Valid Rsyncdepth check failed test
        self.assertRaises(Exception, RsyncSource, 'dummy', netcat=True, rsyncpath='/netcattext')
        # The next 2 should be valid
        self.assertTrue(isinstance(RsyncSource('dummy', netcat=True, rsyncpath='/path/dummy', rsyncdepth=2), RsyncSource))
        self.assertTrue(isinstance(RsyncSource('dummy', rsyncpath='/tmp', rsyncdepth=5), RsyncSource))
        # Controller main class and module/session check
        zkclient = RsyncController('dummy', rsyncpath='/tmp')
        self.assertTrue(isinstance(zkclient, RsyncController))
        self.assertEqual(zkclient.module, 'zkrs-default')
        self.assertEqual(zkclient.watchpath, '/admin/rsync/default/watch')
        zkclient = RsyncController('dummy', rsyncpath='/tmp', session='new')
        self.assertTrue(isinstance(zkclient, RsyncController))
        self.assertEqual(zkclient.module, 'zkrs-new')
        self.assertEqual(zkclient.watchpath, '/admin/rsync/new/watch')

    def test_generate_config(self):
        """ Test the generation of the config file"""
        zkclient = RsyncSource('dummy', netcat=True, rsyncpath='/path/dummy', rsyncdepth=2)
        filen = zkclient.generate_file('/path/dummy/some/path')
        filec = open(filen, "r").read()
        self.assertEqual(filec, 'some/path/')
        self.assertRaises(Exception, zkclient.generate_file, '/path/wrong/path')

    def test_generate_daemon_config(self):
        """ Test the generation of the daemon config file"""
        res = "[zkrs-new]\npath = /tmp\nread only = no\nuid = root\ngid = root\n\n"
        zkclient = RsyncDestination('dummy', rsyncpath='/tmp', session='new')
        filen = zkclient.generate_daemon_config()
        filec = open(filen, "r").read()
        self.assertEqual(filec, res)

    def test_activate_and_pausing_dests(self):
        """ Test the pausing , disabling and activation of destinations """
        zkclient = RsyncDestination('dummy', rsyncpath='/tmp', session='new')
        zkclient.pause()
        val, dum = zkclient.get('/admin/rsync/new/dests/test')
        self.assertEqual(val, '')
        zkclient.activate()
        val, dum = zkclient.get('/admin/rsync/new/dests/test')
        self.assertEqual(val, 'active')
        zkclient.pause()
        val, dum = zkclient.get('/admin/rsync/new/dests/test')
        self.assertEqual(val, 'paused')

        zkclient = RsyncSource('dummy', session='new', netcat=True, rsyncpath='/path/dummy', rsyncdepth=2)
        self.assertFalse(zkclient.dest_is_sane('test'))
        val, dum = zkclient.get('/admin/rsync/new/dests/test')
        self.assertEqual(val, '')
        zkclient.set('/admin/rsync/new/dests/test', 'active')
        self.assertTrue(zkclient.dest_is_sane('test'))
        val, dum = zkclient.get('/admin/rsync/new/dests/test')
        self.assertEqual(val, 'active')
        zkclient.set('/admin/rsync/new/dests/test', 'paused')
        self.assertFalse(zkclient.dest_is_sane('test'))
        val, dum = zkclient.get('/admin/rsync/new/dests/test')
        self.assertEqual(val, 'disabled')

def suite():
     """ returns all the testcases in this module """
     return TestLoader().loadTestsFromTestCase(zkClientTest)

if __name__ == '__main__':
    """Use this __main__ block to help write and test unittests
        just uncomment the parts you need
    """

#     zkclient = RsyncSource('dummy', netcat=True, rsyncpath='/path/dummy', rsyncdepth=2)
#     filen = zkclient.generate_file('/path/dummy/some/path')
#     filei = open(filen, "r").read()
#     print filei
#    filen = zkclient.generate_file('/path/wrong/path')


#
#     kzclient = VscKazooClient('lalala')
#     print kzclient.is_ready()
#     kzclient.set_ready()
#     print kzclient.is_ready()
#     print kzclient.znode_path('test')
#     print kzclient.znode_path('/admin/test')
#
#     kz2 = RsyncSource('dummy', rsyncpath='/tmp/', rsyncdepth=3)
#     dummyq = LockingQueue('foo', 'bar')
#     print kz2.attempt_run('echo test', dummyq)


#
#     zkclient = RsyncController('dummy', rsyncpath='/tmp', session='new')
#     print zkclient.module
#     print zkclient.watchpath


#     zkclient = RsyncDestination('dummy', rsyncpath='/tmp', session='new')
#     zkclient.pause()
#     print zkclient.get('/admin/rsync/new/dests/test')
#     zkclient.activate()
#     print zkclient.get('/admin/rsync/new/dests/test')
#     zkclient.pause()
#     print zkclient.get('/admin/rsync/new/dests/test')
#
#     zkclient = RsyncSource('dummy', session='new', netcat=True, rsyncpath='/path/dummy', rsyncdepth=2)
#     print zkclient.dest_is_sane('test')
#     print zkclient.get('/admin/rsync/new/dests/test')
#     zkclient.set('/admin/rsync/new/dests/test', 'active')
#     print zkclient.dest_is_sane('test')
#     print zkclient.get('/admin/rsync/new/dests/test')
#     zkclient.set('/admin/rsync/new/dests/test', 'paused')
#     print zkclient.dest_is_sane('test')
#     print zkclient.get('/admin/rsync/new/dests/test')

