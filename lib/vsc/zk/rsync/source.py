# -*- coding: latin-1 -*-
#
# Copyright 2013-2016 Ghent University
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
zk.rsync source

@author: Stijn De Weirdt (Ghent University)
@author: Kenneth Waegeman (Ghent University)
"""

import json
import os
import re
import tempfile
import time

from vsc.utils.cache import FileCache
from kazoo.recipe.counter import Counter
from kazoo.recipe.queue import LockingQueue
from vsc.utils.run import RunAsyncLoopLog
from vsc.zk.base import ZKRS_NO_SUCH_SESSION_EXIT_CODE
from vsc.zk.depthwalk import get_pathlist, encode_paths, decode_path
from vsc.zk.rsync.controller import RsyncController

class RsyncSource(RsyncController):
    """
    Class for controlling rsync with Zookeeper. 
    Builds a tree of paths to devide, and effectively rsyncs the subpaths.
    Stops when ready
    """

    BASE_PARTIES = RsyncController.BASE_PARTIES + ['sources']
    NC_RANGE = 15
    SLEEPTIME = 1  # For netcat stub
    TIME_OUT = 5  # waiting for destination
    WAITTIME = 5  # check interval of closure of other clients
    CHECK_WAIT = 20  # wait for path to be available
    RSYNC_STATS = ['Number_of_files', 'Number_of_files_transferred', 'Total_file_size',
                   'Total_transferred_file_size', 'Literal_data', 'Matched_data', 'File_list_size',
                   'Total_bytes_sent', 'Total_bytes_received'];


    def __init__(self, hosts, session=None, name=None, default_acl=None,
                 auth_data=None, rsyncpath=None, rsyncdepth=-1, rsubpaths=None,
                 netcat=False, dryrun=False, delete=False, checksum=False, 
                 hardlinks=False, verbose=False, dropcache=False, timeout=None,
                 excludere=None, excl_usr=None, verifypath=True, done_file=None, arbitopts=None):

        kwargs = {
            'hosts'       : hosts,
            'session'     : session,
            'name'        : name,
            'default_acl' : default_acl,
            'auth_data'   : auth_data,
            'rsyncpath'   : rsyncpath,
            'verifypath'  : verifypath,
            'netcat'      : netcat,
            'dropcache'   : dropcache,
        }
        super(RsyncSource, self).__init__(**kwargs)

        self.lockpath = self.znode_path(self.session + '/lock')
        self.lock = None
        self.path_queue = LockingQueue(self, self.znode_path(self.session + '/pathQueue'))
        self.completed_queue = LockingQueue(self, self.znode_path(self.session + '/completedQueue'))
        self.failed_queue = LockingQueue(self, self.znode_path(self.session + '/failedQueue'))
        self.output_queue = LockingQueue(self, self.znode_path(self.session + '/outputQueue'))

        self.stats_path = '%s/stats' % self.session
        self.init_stats()

        if rsyncdepth < 0:
            self.log.raiseException('Invalid rsync depth: %i' % rsyncdepth)
        else:
            self.rsyncdepth = rsyncdepth
        self.rsync_arbitopts = arbitopts
        self.rsync_checksum = checksum
        self.rsync_delete = delete
        self.rsync_dry = dryrun
        self.rsync_hardlinks = hardlinks
        self.rsync_timeout = timeout
        self.rsync_verbose = verbose
        self.done_file = done_file
        self.excludere = excludere
        self.excl_usr = excl_usr
        self.rsubpaths = rsubpaths

    def init_stats(self):
        self.ensure_path(self.znode_path(self.stats_path))
        self.counters = {};
        for stat in self.RSYNC_STATS:
            self.counters[stat] = Counter(self, self.znode_path('%s/%s' % (self.stats_path, stat)))

    def output_stats(self):
        self.stats = dict([(k, v.value) for k, v in self.counters.items()])
        jstring = json.dumps(self.stats)
        self.log.info('progress stats: %s' % jstring)
        return jstring

    def get_sources(self):
        """ Get all zookeeper clients in this session registered as clients """
        hosts = []
        for host in self.parties['sources']:
            hosts.append(host)
        return hosts

    def acq_lock(self):
        """ Try to acquire lock. Returns true if lock is acquired """
        self.lock = self.Lock(self.lockpath, "")
        return self.lock.acquire(False)

    def release_lock(self):
        """ Release the acquired lock """
        return self.lock.release()

    def start_ready_rwatch(self):
        """ Start a watch other clients can register to, but release lock and exit on error """
        watch = self.start_ready_watch()
        if not watch:
            if len(self.get_all_hosts()) == 1:  # Fix previous unclean shutdown
                self.cleanup()
            self.release_lock()
            self.exit()
            return False
        else:
            return watch

    def build_pathqueue(self):
        """ Build a queue of paths that needs to be rsynced """
        self.log.info('removing old queue and building new queue')
        if self.exists(self.path_queue.path):
            self.delete(self.path_queue.path, recursive=True)
        if self.netcat:
            paths = [str(i) for i in range(self.NC_RANGE)]
            time.sleep(self.SLEEPTIME)
        else:
            tuplpaths = get_pathlist(self.rsyncpath, self.rsyncdepth, exclude_re=self.excludere,
                                     exclude_usr=self.excl_usr, rsubpaths=self.rsubpaths)  # By default don't exclude user files
            paths = encode_paths(tuplpaths)
        self.paths_total = len(paths)
        for path in paths:
            self.path_queue.put(path)  # Put_all can issue a zookeeper connection error with big lists
        self.log.info('pathqueue building finished')
        return self.paths_total

    def isempty_pathqueue(self):
        """ Returns true if all paths in pathqueue are done """
        return len(self.path_queue) == 0

    def output_progress(self, todo):
        self.log.info('Progress: %s of %s paths remaining, %s failed' % (todo, self.paths_total, len(self.failed_queue)))
        self.output_stats()

    def output_clients(self, total, sources):
        dests = total - sources
        sources = sources - 1
        self.log.info('Connected source (slave) clients: %s, connected destination clients: %s', sources, dests)

    def wait_and_keep_progress(self):
        todo_paths = self.paths_total
        total_clients = len(self.get_all_hosts())
        total_sources = len(self.get_sources())
        while not self.isempty_pathqueue():
            todo_new = self.len_paths()
            if todo_paths != todo_new:  # Output progress state
                todo_paths = todo_new
                self.output_progress(todo_paths)
            tot_clients_new = len(self.get_all_hosts())
            src_clients_new = len(self.get_sources())
            if total_clients != tot_clients_new or total_sources != src_clients_new:
                total_clients = tot_clients_new
                total_sources = src_clients_new
                self.output_clients(total_clients, total_sources);
            time.sleep(self.WAITTIME)

    def len_paths(self):
        """ Returns how many elements still in pathQueue """
        return len(self.path_queue)

    def shutdown_all(self):
        """ Send end signal and release lock 
        Make sure other clients are disconnected, clean up afterwards."""
        self.stop_ready_watch()
        self.log.debug('watch set to stop')

        while len(self.get_all_hosts()) > 1:
        #    self.log.debug("clients still connected: %s" % self.get_all_hosts())
            time.sleep(self.WAITTIME)
        self.cleanup()

    def get_state(self):
        """Get the state of a running session"""
        remain = self.len_paths()
        if remain > 0:
            code = 0
            self.log.info('Remaining: %s, Failed: %s' % (remain, len(self.failed_queue)))
        else:
            code = ZKRS_NO_SUCH_SESSION_EXIT_CODE
            self.log.info('No active session')
        return code

    def write_donefile(self, values):
        """ Write a cachefile with some stats about the run when done """

        cache_file = FileCache(self.done_file)
        cache_file.update('stats', values, 0)
        cache_file.close()

    def cleanup(self):
        """ Remove all session nodes in zookeeper after first logging the output queues """

        values = {
            'unfinished' : len(self.path_queue),
            'failed' : len(self.path_queue),
            'completed' : len(self.completed_queue)
        }
        while (len(self.path_queue) > 0):
            self.log.warning('Unfinished Path %s' % self.path_queue.get())
            self.path_queue.consume()
        self.delete(self.dest_queue.path, recursive=True)
        self.delete(self.path_queue.path, recursive=True)

        self.output_stats()
        self.delete(self.znode_path(self.stats_path), recursive=True)

        while (len(self.failed_queue) > 0):
            self.log.error('Failed Path %s' % self.failed_queue.get())
            self.failed_queue.consume()

        while (len(self.completed_queue) > 0):
            self.log.info('Completed Path %s' % self.completed_queue.get())
            self.completed_queue.consume()

        self.log.info('Output:')
        while (len(self.output_queue) > 0):
            self.log.info(self.output_queue.get())
            self.output_queue.consume()

        self.delete(self.completed_queue.path, recursive=True)
        self.delete(self.failed_queue.path, recursive=True)
        self.delete(self.output_queue.path, recursive=True)
        self.remove_ready_watch()
        self.release_lock()
        self.log.info('Cleanup done: Lock, Queues and watch removed')

        if self.done_file:
            self.write_donefile(values)


    def generate_file(self, path):
        """
        Writes the relative path used for the rsync of this path, 
        for use by --files-from. 
        """
        if not path.startswith(self.rsyncpath):
            self.log.raiseException('Invalid path! %s is not a subpath of %s!' % (path, self.rsyncpath))
        else:
            subpath = path[len(self.rsyncpath):]
            subpath = subpath.strip(os.path.sep)

            fd, name = tempfile.mkstemp(dir=self.RSDIR, text=True)
            wfile = os.fdopen(fd, "w")
            wfile.write('%s/' % subpath)
            return name

    def attempt_run(self, path, attempts=3):
        """ Try to run a command x times, on failure add to failed queue """

        attempt = 1
        while (attempt <= attempts):

            dest = self.get_a_dest(attempts)  # Keeps it if not consuming
            if not dest or not self.basepath_ok():
                self.path_queue.put(path, priority=50)  # Keep path in queue
                self.path_queue.consume()  # But stop locking it
                time.sleep(self.TIME_OUT)  # Wait before new attempt
                return 1, None
            port, host, _ = tuple(dest.split(':', 2))

            if self.netcat:
                code, output = self.run_netcat(path, host, port)
            else:
                code, output = self.run_rsync(path, host, port)
            if code == 0:
                self.completed_queue.put(path)
                return code, output
            attempt += 1
            time.sleep(self.WAITTIME)  # Wait before new attempt

        self.log.error('There were issues with path %s!' % path)
        self.failed_queue.put(path)
        return 0, output  # otherwise client get stuck

    def parse_output(self, output):
        """
        Parse the rsync output stats, and when verbose, print the files marked for transmission
        """

        if self.rsync_verbose:
            outp = output.split("%s%s" % (os.linesep, os.linesep))
            self.log.info('Verbose file list output is: %s' % outp[0])
            del outp[0]
            output = os.linesep.join(outp)

        lines = output.splitlines()
        for line in lines:
            keyval = line.split(':')
            if len(keyval) < 2 or keyval[1] == ' ':
                self.log.debug('output line not parsed: %s' % line)
                continue
            key = re.sub(' ', '_', keyval[0])
            val = keyval[1].split()[0]
            val = re.sub(',', '', val)
            if key not in self.RSYNC_STATS:
                self.log.debug('output metric not recognised: %s' % key)
                continue
            self.counters[key] += int(val)

    def get_flags(self, files, recursive):
        """
        Make an array of flags to be used
        """
        # Start rsync recursive or non recursive; archive mode (a) is equivalent to  -rlptgoD (see man rsync)
        flags = ['--stats', '--numeric-ids', '-lptgoD', '--files-from=%s' % files]
        if recursive:
            flags.append('-r')
        if self.rsync_delete:
            flags.append('--delete')
        if self.rsync_checksum:
            flags.append('--checksum')
        if self.rsync_dropcache:
            flags.append('--drop-cache')
        if self.rsync_hardlinks:
            flags.append('--hard-links')
        if self.rsync_timeout:
            flags.extend(['--timeout', str(self.rsync_timeout)])
        if self.rsync_verbose:
            flags.append('--verbose')
        if self.rsync_dry:
            flags.append('-n')
        # This should always be processed last
        if self.rsync_arbitopts:
            arbopts = []
            for arbopt in self.rsync_arbitopts:
                opt = "--%s" % re.sub(':', '=', arbopt, 1)
                arbopts.append(opt)
            self.log.warning('Adding unchecked flags %s' % ' '.join(arbopts))
            flags.extend(arbopts)
        return flags

    def run_rsync(self, encpath, host, port):
        """
        Runs the rsync command with or without recursion, delete or dry-run option.
        It uses the destination module linked with this session.
        """
        path, recursive = decode_path(encpath)
        gfile = self.generate_file(path)
        flags = self.get_flags(gfile, recursive)

        self.log.info('%s is sending path %s to %s %s' % (self.whoami, path, host, port))
        self.log.debug('Used flags: "%s"' % ' '.join(flags))
        command = 'rsync %s %s/ rsync://%s:%s/%s' % (' '.join(flags), self.rsyncpath,
                                                     host, port, self.module)
        code, output = RunAsyncLoopLog.run(command)
        os.remove(gfile)
        parsed = self.parse_output(output)
        return code, parsed

    def run_netcat(self, path, host, port):
        """ Test run with netcat """
        time.sleep(self.SLEEPTIME)
        flags = self.get_flags('nofile', 0)
        command = 'echo %s is sending %s with flags "%s" | nc %s %s' % (self.whoami, path, ' '.join(flags), host, port)

        return RunAsyncLoopLog.run(command)

    def rsync_path(self, path):
        """ start rsync session for given path and destination, returns true if successful """
        if not path:
            self.log.raiseException('Empty path given!')
        elif not isinstance(path, basestring):
            self.log.raiseException('Invalid path: %s !' % path)
        else:
            code, output = self.attempt_run(path)
            if output:
                self.output_queue.put(output)
            return (code == 0)


    def get_a_dest(self, attempts):
        """ Try to get a destination x times """
        attempt = 1
        while (attempt <= attempts):
            dest = self.try_a_dest(self.TIME_OUT)  # Keeps it if not consuming
            if dest:  # We locked a rsync daemon
                self.log.debug('Got destination %s' % dest)
                return dest
            attempt += 1
            time.sleep(self.WAITTIME)

        self.log.warning('Still no destination after %s tries' % attempts)
        return None

    def dest_is_sane(self, dest):
        """ Lock destination state, fetch it and disable if paused """
        return self.dest_state(dest, self.STATUS)

    def handle_dest_state(self, dest, destpath, current_state):
        """ 
        Disable destination by consuming it from queue when paused and return false
        If destination is active, return true
        """
        if current_state == self.STATE_PAUSED:
            self.set_znode(destpath, self.STATE_DISABLED)
            self.dest_queue.consume()
            self.log.debug('Destination %s was disabled and removed from queue' % dest)
            return False
        elif current_state == self.STATE_ACTIVE:
            return True
        else:
            self.log.error('Destination %s is in an unknown state %s' % (dest, current_state))
            return False

    def try_a_dest(self, timeout):
        """ 
        Try to get a destination.
        check if destination is still running, otherwise remove
        """
        if len(self.dest_queue) == 0:
            self.log.debug('Destinations not yet available')
        dest = self.dest_queue.get(timeout)
        if dest:
            port, whoami = tuple(dest.split(':', 1))
            if not self.member_of_party(whoami, 'allsd'):
                self.log.debug('destination is not found in party')
                self.dest_queue.consume()
                return None
            elif not self.dest_is_sane(whoami):
                self.log.debug('recieved destination was paused')
                return None
            else:
                portmap = '%s/portmap/%s' % (self.session, whoami)
                lport, _ = self.get_znode(portmap)
                if port != lport:
                    self.log.error('destination port not valid')  # Should not happen
                    self.dest_queue.consume()
                    return None
        return dest

    def rsync(self, timeout=None):
        """ Get a destination, a path and call a new rsync iteration """
        if self.verifypath:
            if not self.basepath_ok():
                self.log.warning('Basepath not available, waiting')
                time.sleep(self.CHECK_WAIT)
                return None
        path = self.path_queue.get(timeout)
        if path:
            if self.rsync_path(path):
                    self.path_queue.consume()
