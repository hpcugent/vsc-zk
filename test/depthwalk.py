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
Unit tests for depthwalk

@author: Kenneth Waegemam (Ghent University)
"""
import os
import re
import shutil
import tempfile

import vsc.zk.depthwalk as dw

from unittest import TestCase, TestLoader

class DepthWalkTest(TestCase):
    """Tests for depthwalk"""

    def setUp(self):
        """ Create a test direcory structure """
        self.basedir = tempfile.mkdtemp()
        dirs = ['a1', 'b1', 'c1', 'a1/aa2', 'a1/ab2', 'a1/ac2', 'b1/ba2', 'b1/bb2', 'a1/ab2/aa3']
        for dir in dirs:
            path = '%s/%s' % (self.basedir, dir)
            os.mkdir(path)
            os.mkdir('%s/.snapshots' % path)
            open('%s/foofile' % path, 'w').close()

    def test_depthwalk(self):
        """Test the depthwalk functionality"""
        pathlist = []
        for root, dirs, files in dw.depthwalk(self.basedir, 3):
            root = re.sub('/tmp/[^/]+', '/tree', root)  # tmpdir to dummy
            pathlist.append(root)
            for dir in dirs:
                pathlist.append(os.path.join(root, dir))
            for file in files:
                    pathlist.append(os.path.join(root, file))

        res = ['/tree', '/tree/c1', '/tree/b1', '/tree/a1', '/tree/c1', '/tree/c1/.snapshots',
               '/tree/c1/foofile', '/tree/c1/.snapshots', '/tree/b1', '/tree/b1/bb2', '/tree/b1/ba2',
               '/tree/b1/.snapshots', '/tree/b1/foofile', '/tree/b1/bb2', '/tree/b1/bb2/.snapshots',
               '/tree/b1/bb2/foofile', '/tree/b1/ba2', '/tree/b1/ba2/.snapshots', '/tree/b1/ba2/foofile',
               '/tree/b1/.snapshots', '/tree/a1', '/tree/a1/ac2', '/tree/a1/ab2', '/tree/a1/aa2',
               '/tree/a1/.snapshots', '/tree/a1/foofile', '/tree/a1/ac2', '/tree/a1/ac2/.snapshots',
               '/tree/a1/ac2/foofile', '/tree/a1/ab2', '/tree/a1/ab2/aa3', '/tree/a1/ab2/.snapshots',
               '/tree/a1/ab2/foofile', '/tree/a1/aa2', '/tree/a1/aa2/.snapshots', '/tree/a1/aa2/foofile',
               '/tree/a1/.snapshots']

        self.assertListEqual(pathlist , res)

    def test_get_pathlist(self):
        """ Tests the functionality of get_pathlist with an exclude rule """
        regex = re.compile('/\.snapshots(/.*|$)')
        res = [('/tree', 0), ('/tree/c1', 0), ('/tree/b1', 0), ('/tree/a1', 0), ('/tree/b1/bb2', 0),
               ('/tree/b1/ba2', 0), ('/tree/a1/ac2', 0), ('/tree/a1/ab2', 0), ('/tree/a1/aa2', 0),
               ('/tree/a1/ab2/aa3', 1)]
        genlist = [(re.sub('/tmp/[^/]+', '/tree', gen), rec) for (gen, rec) in dw.get_pathlist(self.basedir, 3, exclude_re=regex, exclude_usr=None)]
        self.assertListEqual(genlist , res)

    def test_encode_paths(self):
        """ Test the encoding of a pathlist """
        arrin = [('/tree/c1', 0), ('/tree/b1/bb2/.snapshots', 1)]
        self.assertListEqual(dw.encode_paths(arrin), ['0_/tree/c1', '1_/tree/b1/bb2/.snapshots'])

    def test_decode_path(self):
        """ Test the decoding of a path """
        self.assertTupleEqual(dw.decode_path('0_/tree/c1'), ('/tree/c1', 0))
        self.assertTupleEqual(dw.decode_path('1_/tree/b1/bb2/.snapshots'), ('/tree/b1/bb2/.snapshots', 1))

    def tearDown(self):
        shutil.rmtree(self.basedir)

def suite():
    """ returns all the testcases in this module """
    return TestLoader().loadTestsFromTestCase(DepthWalkTest)

if __name__ == '__main__':
    """Use this __main__ block to help write and test unittests
        just uncomment the parts you need
    """
#     basedir = tempfile.mkdtemp()
#     dirs = ['a1', 'b1', 'c1', 'a1/aa2', 'a1/ab2', 'a1/ac2', 'b1/ba2', 'b1/bb2', 'a1/ab2/aa3']
#     for dir in dirs:
#             path = '%s/%s' % (basedir, dir)
#             os.mkdir(path)
#             os.mkdir('%s/.snapshots' % path)
#             open('%s/foofile' % path, 'w').close()
#
#     pathlist = []
#     for root, dirs, files in dw.depthwalk(basedir, 3):
#         root = re.sub('/tmp/[^/]+', '/tree', root)
#         pathlist.append(root)
#         for dir in dirs:
#             pathlist.append(os.path.join(root, dir))
#         for file in files:
#             pathlist.append(os.path.join(root, file))
#     print pathlist
#     regex = re.compile('/\.snapshots(/.*|$)')
#     genlist = [(re.sub('/tmp/[^/]+', '/tree', gen), rec) for (gen, rec) in dw.get_pathlist(basedir, 3, exclude_re=regex, exclude_usr=None)]
#     print genlist
#     lala = [('/tree/c1', 0), ('/tree/b1/bb2/.snapshots', 1)]
#     print dw.encode_paths(lala)
#     print dw.decode_path('0_/tree/c1')
#     print dw.decode_path('1_/tree/b1/bb2/.snapshots')
#
#     shutil.rmtree(basedir)
