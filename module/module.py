#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2009-2012:
#    Gabes Jean, naparuba@gmail.com
#    Gerhard Lausser, Gerhard.Lausser@consol.de
#    Gregory Starck, g.starck@gmail.com
#    Hartmut Goebel, h.goebel@goebel-consult.de
#
# This file is part of Shinken.
#
# Shinken is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Shinken is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Shinken.  If not, see <http://www.gnu.org/licenses/>.

"""
This is a scheduler module to save host/service retention data into a mongodb database
"""

import time
import cPickle
import datetime

import base64
from multiprocessing import Process, cpu_count

try:
    from pymongo import MongoClient,version
except ImportError:
    MongoClient = None

from shinken.basemodule import BaseModule
from shinken.log import logger

properties = {
    'daemons': ['scheduler'],
    'type': 'mongodb_retention',
    'external': False,
    }


def get_instance(plugin):
    """
    Called by the plugin manager to get a broker
    """
    logger.debug("Get a Mongodb retention scheduler module for plugin %s" %
                 plugin.get_name())
    if not MongoClient:
        raise Exception('Could not use the pymongo module. Please verify your '
                        'pymongo install.')
    uri = plugin.uri
    database = plugin.database
    username = getattr(plugin, 'username', '')
    password = getattr(plugin, 'password', '')
    replica_set = getattr(plugin, 'replica_set', '')
    max_workers = getattr(plugin, 'max_workers', 4)
    if not isinstance(max_workers, int) and not max_workers.isdigit():
        logger.error('[MongodbRetention] Invalid max_workers parameter. '
                     'Defaulting to default (4).')
        max_workers = 4
    max_workers = int(max_workers)
    worker_timeout = getattr(plugin, 'worker_timeout', 30)
    if not isinstance(worker_timeout, int) and not worker_timeout.isdigit():
        logger.error('[MongodbRetention] Invalid worker_timeout parameter. '
                     'Defaulting to default (4).')
        worker_timeout = 30
    worker_timeout = int(worker_timeout)
    if worker_timeout == 0:
        worker_timeout = None
    instance = Mongodb_retention_scheduler(plugin, uri, database, username,
                                           password, replica_set, max_workers,
                                           worker_timeout)
    return instance


def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


class Mongodb_retention_scheduler(BaseModule):
    def __init__(self, modconf, uri, database, username, password, replica_set,
                 max_workers, worker_timeout):
        BaseModule.__init__(self, modconf)
        self.uri = uri
        self.database = database
        self.username = username
        self.password = password
        self.replica_set = replica_set
        self.max_workers = max_workers
        self.worker_timeout = worker_timeout
        # Older versions don't handle replicasets and don't have the fsync
        # option
        if version < 2:
            logger.error('[MongodbRetention] Your pymongo lib is too old. '
                         'Please install at least a 2.x+ version.')
            return None

    def init(self):
        """
        Called by Scheduler to say 'let's prepare yourself guy'
        """
        logger.debug("Initialization of the mongodb module")

        if self.replica_set:
            self.con = MongoClient(self.uri, replicaSet=self.replica_set,
                                   fsync=False)
        else:
            self.con = MongoClient(self.uri, fsync=False)

        self.db = getattr(self.con, self.database)
        if self.username != '' and self.password != '':
            self.db.authenticate(self.username, self.password)
        self.hosts_fs = getattr(self.db, 'retention_hosts_raw')
        self.services_fs = getattr(self.db, 'retention_services_raw')

    def job(self, all_data, wid, offset):
        try:
            self._job(all_data, wid, offset)
        except Exception, exc:
            logger.error(exc)
        finally:
            # I'm a subprocess and need to close my forked socket
            self.con.close()

    def _job(self, all_data, wid, offset):
        # Reinit the mongodb connection if need
        self.init()
        all_objs = {'hosts': {}, 'services': {}}
        date = datetime.datetime.utcnow()

        hosts = all_data['hosts']
        services = all_data['services']

        # Prepare the encoding for all managed hosts
        for i, h_name in enumerate(hosts):
            # Only manage the worker id element of the offset (number of
            # workers) elements
            if (i % offset) != wid:
                continue
            h = hosts[h_name]
            key = "HOST-%s" % h_name
            val = cPickle.dumps(h, protocol=cPickle.HIGHEST_PROTOCOL)
            b64_val = base64.b64encode(val)
            # We save it in the Gridfs for hosts
            all_objs['hosts'][key] = {
                '_id': key,
                'value': b64_val,
                'date': date
            }

        for i, (h_name, s_desc) in enumerate(services):
            # Only manage the worker id element of the offset (number of
            # workers) elements
            if (i % offset) != wid:
                continue
            s = services[(h_name, s_desc)]
            key = "SERVICE-%s,%s" % (h_name, s_desc)
            # space are not allowed in a key.. so change it by SPACE token
            key = key.replace(' ', 'SPACE')
            val = cPickle.dumps(s, protocol=cPickle.HIGHEST_PROTOCOL)
            b64_val = base64.b64encode(val)
            all_objs['services'][key] = {
                '_id': key,
                'value': b64_val,
                'date': date
            }

        if len(all_objs['hosts']) != 0:
            # Do bulk insert of 500 elements (~500K insert)
            lsts = list(chunks(all_objs['hosts'].values(), 500))
            for lst in lsts:
                bulk = self.hosts_fs.initialize_unordered_bulk_op()
                for doc in lst:
                    bulk.find({'_id': doc['_id']}).upsert().replace_one(doc)
                bulk.execute({'w': 0})

        if len(all_objs['services']) != 0:
            # Do bulk insert of 500 elements (~500K insert)
            lsts = list(chunks(all_objs['services'].values(), 500))
            for lst in lsts:
                bulk = self.services_fs.initialize_unordered_bulk_op()
                for doc in lst:
                    bulk.find({'_id': doc['_id']}).upsert().replace_one(doc)
                bulk.execute({'w': 0})

        # Return and so quit this sub-process
        return

    def hook_save_retention(self, daemon):
        """
        main function that is called in the retention creation pass
        """
        t0 = time.time()
        logger.debug("[MongodbRetention] asking me to update the retention "
                     "objects")

        all_data = daemon.get_retention_data()

        processes = []
        for i in xrange(self.max_workers):
            proc = Process(
                target=self.job,
                args=(all_data, i, self.max_workers)
            )
            proc.start()
            processes.append(proc)

        # Allow {worker_timeout}s to join the sub-processes
        for proc in processes:
            proc.join(self.worker_timeout)

        for i in xrange(len(processes)):
            proc = processes[i]
            if proc.is_alive():
                logger.error("[MongodbRetention] Worker %d did not terminate "
                             "in time. Perhaps worker_timeout (%d) is too "
                             "small" % (i, self.worker_timeout))
                proc.terminate()

        logger.info("[MongodbRetention] Retention information updated in "
                    "Mongodb (%.2fs)" % (time.time() - t0))

    # Should return if it succeed in the retention load or not
    def hook_load_retention(self, daemon):
        t0 = time.time()

        # Now the new redis way :)
        logger.debug("MongodbRetention] asking me to load the retention "
                     "objects")

        # We got list of loaded data from retention uri
        ret_hosts = {}
        ret_services = {}

        found_hosts = {}
        found_services = {}

        for h in self.hosts_fs.find():
            val = h.get('value', None)
            if val is not None:
                found_hosts[h.get('_id')] = val

        for s in self.services_fs.find():
            val = s.get('value', None)
            if val is not None:
                found_services[s.get('_id')] = val

        for h in daemon.hosts:
            key = "HOST-%s" % h.host_name
            if key in found_hosts:
                val = found_hosts[key]
                val = base64.b64decode(val)
                val = cPickle.loads(val)
                ret_hosts[h.host_name] = val

        for s in daemon.services:
            key = "SERVICE-%s,%s" % (s.host.host_name, s.service_description)
            # space are not allowed in memcache key.. so change it by SPACE
            # token
            key = key.replace(' ', 'SPACE')
            if key in found_services:
                val = found_services[key]
                val = base64.b64decode(val)
                val = cPickle.loads(val)
                ret_services[(s.host.host_name, s.service_description)] = val

        all_data = {'hosts': ret_hosts, 'services': ret_services}

        # Ok, now comme load them scheduler :)
        daemon.restore_retention_data(all_data)

        logger.info("[MongodbRetention] Retention information loaded from "
                    "Mongodb (%.2fs)" % (time.time() - t0))

        return True
