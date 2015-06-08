#!/usr/bin/env python
#coding: utf-8

import redis
from .connection import OverLimitBlockingConnectionPool


class Redis(object):

    def __init__(self, confmap):
        self.__confmap = confmap

    @property
    def url(self):
        network  = self.__confmap.get("network")
        password = self.__confmap.get("password", "")
        dbnum = self.__confmap.get("db", 0)

        return "{network}://{password}@{endpoint}{dbnum}".format(
            network  = network,
            password = ":" + password if password else "",
            endpoint = self.__confmap.get("endpoint"),
            dbnum = "?db={0}".format(dbnum) if network == "unix" else "/{0}".format(dbnum)
        )

    def get_client(self):
        return redis.StrictRedis(
            connection_pool = OverLimitBlockingConnectionPool.from_url(
                self.url,
                max_connections = self.__confmap.get("maxActive", 100),
                max_overlimit = self.__confmap.get("maxIdle", 100),
                timeout = self.__confmap.get("timeout", 20),
            )
        )
