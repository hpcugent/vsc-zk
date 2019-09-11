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
Unit tests for Vsc-zk Counters

@author: Kenneth Waegeman (Ghent University)
"""
import sys
import time

# same as in zkclient
sys.modules['kazoo.client'] = __import__('mocky')
sys.modules['kazoo.recipe.queue'] = __import__('mocky')
sys.modules['kazoo.recipe.party'] = __import__('mocky')
sys.modules['kazoo.recipe.counter'] = __import__('mocky')

from kazoo.client import KazooClient
from kazoo.recipe.party import Party
from kazoo.recipe.queue import LockingQueue
from vsc.zk.rsync.source import RsyncSource

from vsc.install.testing import TestCase

rsync_output = """
2014-06-02 17:25:20,684 INFO       zkrsync.RsyncSource MainThread
sending incremental file list
file
anotherfile
bla

Number of files: 55 (reg: 54, dir: 1)
Number of regular files transferred: 17
Total file size: 39488 bytes
Total transferred file size: 39488 bytes
Literal data: 39488 bytes
Matched data: 0 bytes
File list size: 371
File list generation time: 0.001 seconds
File list transfer time: 0.000 seconds
Total bytes sent: 40610
Total bytes received: 342

sent 40610 bytes  received 342 bytes  81904.00 bytes/sec
total size is 39488  speedup is 0.96
"""

json_output = '{"Total_transferred_file_size": 39488, "Total_file_size": 39488, "File_list_size": 371, "Total_bytes_sent": 40610, "Literal_data": 39488, "Number_of_files": 55, "Total_bytes_received": 342, "Matched_data": 0, "Number_of_regular_files_transferred": 17}'
json_output2 = '{"Total_transferred_file_size": 78976, "Total_file_size": 78976, "File_list_size": 742, "Total_bytes_sent": 81220, "Literal_data": 78976, "Number_of_files": 110, "Total_bytes_received": 684, "Matched_data": 0, "Number_of_regular_files_transferred": 34}'

class zkStatCounterTest(TestCase):

    def test_generate_stats(self):
        """ Test the correct working of stats gathering and outputting """
        zkclient = RsyncSource('dummy', netcat=True, rsyncpath='/path/dummy', rsyncdepth=2, verbose=True)

        zkclient.parse_output(rsync_output)
        self.assertEqual(zkclient.output_stats(), json_output)
        zkclient.parse_output(rsync_output)
        self.assertEqual(zkclient.output_stats(), json_output2)
