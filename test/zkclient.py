#
# Copyright 2012-2023 Ghent University
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
Unit tests for VscKazooClient and childs

@author: Kenneth Waegeman (Ghent University)
"""
import sys
import mock

from pathlib import Path

from kazoo.client import KazooClient
from kazoo.recipe.party import Party
from kazoo.recipe.queue import LockingQueue

# same as in statcounter
sys.modules['kazoo.client'] = __import__('mocky')
sys.modules['kazoo.recipe.queue'] = __import__('mocky')
sys.modules['kazoo.recipe.party'] = __import__('mocky')
sys.modules['kazoo.recipe.counter'] = __import__('mocky')

from vsc.install.testing import TestCase
from vsc.utils.cache import FileCache
from vsc.zk.base import VscKazooClient, RunWatchLoopLog, ZKRS_NO_SUCH_SESSION_EXIT_CODE
from vsc.zk.rsync.controller import RsyncController
from vsc.zk.rsync.destination import RsyncDestination
from vsc.zk.rsync.source import RsyncSource

class zkClientTest(TestCase):

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
        self.assertTrue(isinstance(RsyncSource('dummy', netcat=True, rsyncpath='/path/dummy', rsyncdepth=2),
                                   RsyncSource))
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
        filec = Path(filen).read_text(encoding='utf8')
        self.assertEqual(filec, 'some/path/')
        self.assertRaises(Exception, zkclient.generate_file, '/path/wrong/path')

    def test_generate_daemon_config(self):
        """ Test the generation of the daemon config file"""
        res = "[zkrs-new]\npath = /tmp\nread only = no\nuid = root\ngid = root\n\n"
        zkclient = RsyncDestination('dummy', rsyncpath='/tmp', session='new')
        filen = zkclient.generate_daemon_config()
        filec = Path(filen).read_text(encoding='utf8')
        self.assertEqual(filec, res)

    def test_activate_and_pausing_dests(self):
        """ Test the pausing , disabling and activation of destinations """
        zkclient = RsyncDestination('dummy', rsyncpath='/tmp', session='new')
        zkclient.pause()
        val, _ = zkclient.getstr('/admin/rsync/new/dests/test')
        self.assertEqual(val, '')
        zkclient.activate()
        val, _ = zkclient.getstr('/admin/rsync/new/dests/test')
        self.assertEqual(val, 'active')
        zkclient.pause()
        val, _ = zkclient.getstr('/admin/rsync/new/dests/test')
        self.assertEqual(val, 'paused')

        zkclient = RsyncSource('dummy', session='new', netcat=True, rsyncpath='/path/dummy', rsyncdepth=2)
        self.assertFalse(zkclient.dest_is_sane('test'))
        val, _ = zkclient.getstr('/admin/rsync/new/dests/test')
        self.assertEqual(val, '')
        zkclient.setstr('/admin/rsync/new/dests/test', 'active')
        self.assertTrue(zkclient.dest_is_sane('test'))
        val, _ = zkclient.getstr('/admin/rsync/new/dests/test')
        self.assertEqual(val, 'active')
        zkclient.setstr('/admin/rsync/new/dests/test', 'paused')
        self.assertFalse(zkclient.dest_is_sane('test'))
        val, _ = zkclient.getstr('/admin/rsync/new/dests/test')
        self.assertEqual(val, 'disabled')

    def test_wirte_donefile(self):
        """ Test the writing of the values to a cache file when done"""
        donefile = "/tmp/done"
        values = {
            'completed' : 50,
            'failed' : 5,
            'unfinished' : 0
        }
        zkclient = RsyncSource('dummy', session='new', netcat=True, rsyncpath='/path/dummy',
                               rsyncdepth=2, done_file=donefile)
        zkclient.write_donefile(values)
        cache_file = FileCache(donefile)
        (_, stats) = cache_file.load('stats')
        self.assertEqual(values, stats)


    @mock.patch('vsc.zk.rsync.source.RsyncSource.len_paths')
    def test_get_state(self, mock_len):

        zkclient = RsyncSource('dummy', session='new', netcat=True, rsyncpath='/path/dummy', rsyncdepth=2)
        mock_len.return_value = 5
        self.assertEqual(zkclient.get_state(), 0)
        mock_len.return_value = 0
        self.assertEqual(zkclient.get_state(), ZKRS_NO_SUCH_SESSION_EXIT_CODE)
