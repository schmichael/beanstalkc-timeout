#!/usr/bin/env python
"""beanstalkc - A beanstalkd Client Library for Python"""

__license__ = '''
Copyright (C) 2008-2011 Andreas Bolka
Copyright (C) 2012 Urban Airship

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

__version__ = '0.2.0'
import errno
import select
import socket

import yaml


DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 11300
DEFAULT_PRIORITY = 2**31
DEFAULT_TTR = 120
DEFAULT_TIMEOUT = socket.getdefaulttimeout() or 2
DEFAULT_SO_KEEPALIVE = False


class Beanstalkc2Exception(Exception): pass
class ConnectionClosed(Beanstalkc2Exception):
    """beanstalkd server connection unexpectedly closed"""
class CommandFailed(Beanstalkc2Exception): pass
class UnexpectedResponse(Beanstalkc2Exception): pass
class DeadlineSoon(Exception): pass


class Connection(object):
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT,
                 timeout=DEFAULT_TIMEOUT, keepalives=DEFAULT_SO_KEEPALIVE):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.keepalives = keepalives
        self.buf = bytearray()
        self.connect()

    def connect(self):
        """Connect to beanstalkd server."""
        self._socket = socket.create_connection(
                (self.host, self.port), self.timeout)
        self._socket.setblocking(0)
        if self.keepalives:
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self._select_socket = [self._socket.fileno()]

    def close(self):
        """Close connection to server."""
        try:
            self._sendall('quit\r\n')
            self._socket.close()
        except socket.error:
            pass

    def _sendall(self, data):
        try:
            sent = self._socket.send(data)
        except socket.error as e:
            if e.errno == errno.EAGAIN:
                # EAGAIN means we just need to select and try again
                sent = 0
            else:
                raise
        remaining = len(data) - sent
        while remaining:
            _, w, _ = select.select([], self._select_socket, [], self.timeout)
            if not w:
                raise socket.timeout('timed out')
            try:
                sent += self._socket.send(data[sent:])
            except socket.error as e:
                # EAGAINs are safe to ignore
                if e.errno != errno.EAGAIN:
                    raise
            else:
                remaining -= sent

    def _interact(self, command, expected_ok, expected_err=(), timeout=None):
        if timeout is None:
            timeout = self.timeout
        self._sendall(command)
        status, results = self._read_response(timeout)
        if status in expected_ok:
            return results
        elif status in expected_err:
            raise CommandFailed(command.split()[0], status, results)
        else:
            raise UnexpectedResponse(command.split()[0], status, results)

    def _read_response(self, timeout):
        while 1:
            r, _, _ = select.select(self._select_socket, [], [], timeout)
            if not r:
                raise socket.timeout('timed out')
            chunk = self._socket.recv(256)
            if not chunk:
                raise ConnectionClosed(
                        '%s:%s connection closed' % (self.host, self.port))
            self.buf.extend(chunk)
            line, _, self.buf = self.buf.partition('\r\n')
            if line:
                # Line received, return
                response = line.split()
                return response[0], response[1:]

    def _read_body(self, size):
        full_size = size + 2  # trailing "\r\n"
        remaining = full_size - len(self.buf)
        while remaining > 0:
            r, _, _ = select.select(self._select_socket, [], [], self.timeout)
            if not r:
                raise socket.timeout('timed out')
            chunk = self._socket.recv(remaining)
            if not chunk:
                raise ConnectionClosed(
                        '%s:%s connection closed' % (self.host, self.port))
            self.buf.extend(chunk)
            remaining = full_size - len(self.buf)
        body = str(self.buf[:size])
        self.buf = self.buf[full_size:]
        return body

    def _interact_value(self, command, expected_ok, expected_err=()):
        return self._interact(command, expected_ok, expected_err)[0]

    def _interact_job(self, command, expected_ok, expected_err, reserved=True,
                      timeout=None):
        jid, size = self._interact(
                command, expected_ok, expected_err, timeout=timeout)
        body = self._read_body(int(size))
        return Job(self, int(jid), body, reserved)

    def _interact_yaml(self, command, expected_ok, expected_err=()):
        size, = self._interact(command, expected_ok, expected_err)
        body = self._read_body(int(size))
        return yaml.load(body)

    def _interact_peek(self, command):
        try:
            return self._interact_job(command, ['FOUND'], ['NOT_FOUND'], False)
        except CommandFailed, (_, status, results):
            return None

    # -- public interface --

    def put(self, body, priority=DEFAULT_PRIORITY, delay=0, ttr=DEFAULT_TTR):
        """Put a job into the current tube. Returns job id."""
        assert isinstance(body, str), 'Job body must be a str instance'
        jid = self._interact_value(
                'put %d %d %d %d\r\n%s\r\n' %
                    (priority, delay, ttr, len(body), body),
                ['INSERTED', 'BURIED'], ['JOB_TOO_BIG'])
        return int(jid)

    def reserve(self, timeout=None):
        """Reserve a job from one of the watched tubes, with optional timeout
        in seconds. Returns a Job object, or None if the request times out."""
        if timeout is not None:
            command = 'reserve-with-timeout %d\r\n' % timeout
            socket_timeout = timeout + self.timeout
        else:
            command = 'reserve\r\n'
            socket_timeout = timeout
        try:
            return self._interact_job(command,
                                      ['RESERVED'],
                                      ['DEADLINE_SOON', 'TIMED_OUT'],
                                      timeout=socket_timeout)
        except CommandFailed, (_, status, results):
            if status == 'TIMED_OUT':
                return None
            elif status == 'DEADLINE_SOON':
                raise DeadlineSoon(results)

    def kick(self, bound=1):
        """Kick at most bound jobs into the ready queue."""
        return int(self._interact_value('kick %d\r\n' % bound, ['KICKED']))

    def peek(self, jid):
        """Peek at a job. Returns a Job, or None."""
        return self._interact_peek('peek %d\r\n' % jid)

    def peek_ready(self):
        """Peek at next ready job. Returns a Job, or None."""
        return self._interact_peek('peek-ready\r\n')

    def peek_delayed(self):
        """Peek at next delayed job. Returns a Job, or None."""
        return self._interact_peek('peek-delayed\r\n')

    def peek_buried(self):
        """Peek at next buried job. Returns a Job, or None."""
        return self._interact_peek('peek-buried\r\n')

    def tubes(self):
        """Return a list of all existing tubes."""
        return self._interact_yaml('list-tubes\r\n', ['OK'])

    def using(self):
        """Return a list of all tubes currently being used."""
        return self._interact_value('list-tube-used\r\n', ['USING'])

    def use(self, name):
        """Use a given tube."""
        return self._interact_value('use %s\r\n' % name, ['USING'])

    def watching(self):
        """Return a list of all tubes being watched."""
        return self._interact_yaml('list-tubes-watched\r\n', ['OK'])

    def watch(self, name):
        """Watch a given tube."""
        return int(self._interact_value('watch %s\r\n' % name, ['WATCHING']))

    def ignore(self, name):
        """Stop watching a given tube."""
        try:
            return int(self._interact_value('ignore %s\r\n' % name,
                                            ['WATCHING'],
                                            ['NOT_IGNORED']))
        except CommandFailed:
            return 1

    def stats(self):
        """Return a dict of beanstalkd statistics."""
        return self._interact_yaml('stats\r\n', ['OK'])

    def stats_tube(self, name):
        """Return a dict of stats about a given tube."""
        return self._interact_yaml('stats-tube %s\r\n' % name,
                                  ['OK'],
                                  ['NOT_FOUND'])

    def pause_tube(self, name, delay):
        """Pause a tube for a given delay time, in seconds."""
        self._interact('pause-tube %s %d\r\n' % (name, delay),
                       ['PAUSED'],
                       ['NOT_FOUND'])

    # -- job interactors --

    def delete(self, jid):
        """Delete a job, by job id."""
        self._interact('delete %d\r\n' % jid, ['DELETED'], ['NOT_FOUND'])

    def release(self, jid, priority=DEFAULT_PRIORITY, delay=0):
        """Release a reserved job back into the ready queue."""
        self._interact('release %d %d %d\r\n' % (jid, priority, delay),
                       ['RELEASED', 'BURIED'],
                       ['NOT_FOUND'])

    def bury(self, jid, priority=DEFAULT_PRIORITY):
        """Bury a job, by job id."""
        self._interact('bury %d %d\r\n' % (jid, priority),
                       ['BURIED'],
                       ['NOT_FOUND'])

    def touch(self, jid):
        """Touch a job, by job id, requesting more time to work on a reserved
        job before it expires."""
        self._interact('touch %d\r\n' % jid, ['TOUCHED'], ['NOT_FOUND'])

    def stats_job(self, jid):
        """Return a dict of stats about a job, by job id."""
        return self._interact_yaml('stats-job %d\r\n' % jid,
                                   ['OK'],
                                   ['NOT_FOUND'])


class Job(object):
    def __init__(self, conn, jid, body, reserved=True):
        self.conn = conn
        self.jid = jid
        self.body = body
        self.reserved = reserved

    def _priority(self):
        stats = self.stats()
        if isinstance(stats, dict):
            return stats['pri']
        return DEFAULT_PRIORITY

    # -- public interface --

    def delete(self):
        """Delete this job."""
        self.conn.delete(self.jid)
        self.reserved = False

    def release(self, priority=None, delay=0):
        """Release this job back into the ready queue."""
        if self.reserved:
            self.conn.release(self.jid, priority or self._priority(), delay)
            self.reserved = False

    def bury(self, priority=None):
        """Bury this job."""
        if self.reserved:
            self.conn.bury(self.jid, priority or self._priority())
            self.reserved = False

    def touch(self):
        """Touch this reserved job, requesting more time to work on it before
        it expires."""
        if self.reserved:
            self.conn.touch(self.jid)

    def stats(self):
        """Return a dict of stats about this job."""
        return self.conn.stats_job(self.jid)


if __name__ == '__main__':
    import doctest, os, signal
    try:
        pid = os.spawnlp(os.P_NOWAIT,
                         'beanstalkd',
                         'beanstalkd', '-l', '127.0.0.1', '-p', '14711')
        doctest.testfile('TUTORIAL.mkd', optionflags=doctest.ELLIPSIS)
        doctest.testfile('test/no-yaml.doctest', optionflags=doctest.ELLIPSIS)
    finally:
        os.kill(pid, signal.SIGTERM)
