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
Unit tests for depthwalk

@author: Kenneth Waegeman (Ghent University)
"""
import os
import re
import shutil
import tempfile

import vsc.zk.depthwalk as dw

from vsc.install.testing import TestCase

class DepthWalkTest(TestCase):
    """Tests for depthwalk"""

    def setUp(self):
        """ Create a test direcory structure """
        super().setUp()

        self.basedir = tempfile.mkdtemp()
        dirs = ['a1', 'b1', 'c1', 'a1/aa2', 'a1/ab2', 'a1/ac2', 'b1/ba2', 'b1/bb2', 'a1/ab2/aa3']
        extradirs = ['a1/ab2/aa3/sub1', 'a1/ab2/aa3/sub2', 'a1/ab2/aa3/sub2/sub21',
                     'a1/ab2/aa3/sub2/sub21/sub211', 'a1/ab2/aa3/sub2/sub21/sub211/sub2112']
        dirs.extend(extradirs)

        for dirn in dirs:
            path = f'{self.basedir}/{dirn}'
            os.mkdir(path)
            os.mkdir(f'{path}/.snapshots')
            open(f'{path}/foofile', 'w', encoding='utf8').close()

    def tearDown(self):
        shutil.rmtree(self.basedir)
        super().tearDown()

    def test_depthwalk(self):
        """Test the depthwalk functionality"""
        pathlist = []
        for root, dirs, files in dw.depthwalk(self.basedir, 3):
            root = re.sub('(/var)?/tmp/[^/]+', '/tree', root)  # tmpdir to dummy
            pathlist.append(root)
            for dirn in dirs:
                pathlist.append(os.path.join(root, dirn))
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

        self.assertEqual(sorted(pathlist) , sorted(res))

    def test_get_pathlist(self):
        """ Tests the functionality of get_pathlist with an exclude rule """
        regex = re.compile(r'/\.snapshots(/.*|$)')
        res = [('/tree', 0), ('/tree/c1', 0), ('/tree/b1', 0), ('/tree/a1', 0), ('/tree/b1/bb2', 0),
               ('/tree/b1/ba2', 0), ('/tree/a1/ac2', 0), ('/tree/a1/ab2', 0), ('/tree/a1/aa2', 0),
               ('/tree/a1/ab2/aa3', 1)]
        genlist = [(re.sub('(/var)?/tmp/[^/]+', '/tree', gen), rec) for (gen, rec) in
                   dw.get_pathlist(self.basedir, 3, exclude_re=regex, exclude_usr=None)]
        self.assertEqual(sorted(genlist) , sorted(res))

    def test_get_pathlist_with_subpaths(self):
        """ Tests the functionality of get_pathlist with an exclude rule and subpaths"""
        regex = re.compile(r'/\.snapshots(/.*|$)')
        res = [('/tree', 0), ('/tree/c1', 0), ('/tree/b1', 0), ('/tree/a1', 0), ('/tree/b1/bb2', 0),
               ('/tree/b1/ba2', 0), ('/tree/a1/ac2', 0), ('/tree/a1/ab2', 0), ('/tree/a1/aa2', 0),
               ('/tree/a1/ab2/aa3', 0), ('/tree/a1/ab2/aa3/sub1', 0), ('/tree/a1/ab2/aa3/sub2', 0),
               ('/tree/a1/ab2/aa3/sub2/sub21', 0), ('/tree/a1/ab2/aa3/sub2/sub21/sub211', 1), ]

        subpath1 = '3_a1/ab2/aa3'
        subpath2 = '3_a1/ab2/aa3/sub2'
        self.assertRaises(Exception, dw.get_pathlist, self.basedir, 3, exclude_re=regex,
                          exclude_usr=None, rsubpaths=['3_/a1/ab2/aa3'])
        self.assertRaises(Exception, dw.get_pathlist, self.basedir, 2, exclude_re=regex,
                          exclude_usr=None, rsubpaths=['3_a1/ab2/aa3'])
        self.assertRaises(Exception, dw.get_pathlist, self.basedir, 3, exclude_re=regex,
                          exclude_usr=None, rsubpaths=['1_a1'])

        genlist = [(re.sub('(/var)?/tmp/[^/]+', '/tree', gen), rec) for (gen, rec) in
                   dw.get_pathlist(self.basedir, 3, exclude_re=regex, rsubpaths=[subpath1])]
        self.assertEqual(sorted(genlist), sorted(res))

        genlist = [(re.sub('(/var)?/tmp/[^/]+', '/tree', gen), rec)
                   for (gen, rec) in dw.get_pathlist(self.basedir, 3, exclude_re=regex, rsubpaths=[subpath1, subpath2])]
        res.remove(('/tree/a1/ab2/aa3/sub2/sub21/sub211', 1))
        res.extend([('/tree/a1/ab2/aa3/sub2/sub21/sub211', 0), ('/tree/a1/ab2/aa3/sub2/sub21/sub211/sub2112', 1)])
        self.assertEqual(sorted(genlist), sorted(res))

        subpath2 = '4_a1/ab2/aa3/sub2'
        genlist = [(re.sub('(/var)?/tmp/[^/]+', '/tree', gen), rec)
                   for (gen, rec) in dw.get_pathlist(self.basedir, 3, exclude_re=regex, rsubpaths=[subpath1, subpath2])]
        res.remove(('/tree/a1/ab2/aa3/sub2/sub21/sub211/sub2112', 1))
        res.append(('/tree/a1/ab2/aa3/sub2/sub21/sub211/sub2112', 0))
        self.assertEqual(sorted(genlist), sorted(res))

        subpath1 = '3_a1/ab2'
        subpath2 = '3_a1/ab2/aa3'
        self.assertRaises(Exception, dw.get_pathlist, self.basedir, 3, exclude_re=regex,
                          exclude_usr=None, rsubpaths=[subpath2, subpath1])

    def test_encode_paths(self):
        """ Test the encoding of a pathlist """
        arrin = [('/tree/c1', 0), ('/tree/b1/bb2/.snapshots', 1)]
        self.assertEqual(dw.encode_paths(arrin), ['0_/tree/c1', '1_/tree/b1/bb2/.snapshots'])

    def test_decode_path(self):
        """ Test the decoding of a path """
        self.assertEqual(dw.decode_path('0_/tree/c1'), ('/tree/c1', 0))
        self.assertEqual(dw.decode_path('1_/tree/b1/bb2/.snapshots'), ('/tree/b1/bb2/.snapshots', 1))
