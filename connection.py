#!/usr/bin/env python
# encoding: utf-8

import os
from redis.connection import ConnectionPool, Connection
from redis._compat import LifoQueue, Full, Empty
from redis.exceptions import ConnectionError
import threading



class OverLimitBlockingConnectionPool(ConnectionPool):
   
    def __init__(self, max_connections=50, max_overlimit=50, timeout=20,
                 connection_class=Connection, queue_class=LifoQueue,
                 **connection_kwargs):

        self.queue_class = queue_class
        self.timeout = timeout
        self.max_overlimit = 50
        super(OverLimitBlockingConnectionPool, self).__init__(
            connection_class=connection_class,
            max_connections=max_connections,
            **connection_kwargs)
        self._created_connections = 0

    def reset(self):
        self.pid = os.getpid()
        self._check_lock = threading.Lock()

        self.pool = self.queue_class(self.max_connections + self.max_overlimit)
        while True:
            try:
                self.pool.put_nowait(None)
            except Full:
                break

        self._connections = []

    def make_connection(self):
        connection = self.connection_class(**self.connection_kwargs)
        self._connections.append(connection)
        self._created_connections += 1
        return connection

    def get_connection(self, command_name, *keys, **options):
        # Make sure we haven't changed process.
        self._checkpid()

        # Try and get a connection from the pool. If one isn't available within
        # self.timeout then raise a ``ConnectionError``.
        connection = None
        try:
            connection = self.pool.get(block=True, timeout=self.timeout)
        except Empty:
            raise ConnectionError("No connection available.")

        # If the ``connection`` is actually ``None`` then that's a cue to make
        # a new connection to add to the pool.
        if connection is None:
            connection = self.make_connection()

        return connection

    def release(self, connection):
        "Releases the connection back to the pool."
        # Make sure we haven't changed process.
        self._checkpid()
        if connection.pid != self.pid:
            return

        # Put the connection back into the pool.
        if self._created_connections > self.max_connections:
            self._created_connections -= 1
            connection.disconnect()
            return

        try:
            self.pool.put_nowait(connection)
        except Full:
            # perhaps the pool has been reset() after a fork? regardless,
            # we don't want this connection
            pass

    def disconnect(self):
        "Disconnects all connections in the pool."
        for connection in self._connections:
            connection.disconnect()
