#!/usr/bin/env python
#
# Copyright (c) 2017 Belmiro Moreira
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
# Author:
#  Belmiro Moreira <moreira.belmiro@gmail.com>

import argparse
import ConfigParser
import os
import subprocess
import sys
import datetime

from sqlalchemy import and_
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

KILO_VERSION = 286
LIBERTY_VERSION = 302
MITAKA_VERSION = 319

class DB_query:
    def __init__(self, db_url):
        engine = create_engine(db_url)
        engine.connect()
        Session = sessionmaker(bind=engine)
        db_session = Session()
        db_metadata = MetaData()
        db_metadata.bind = engine
        db_base = declarative_base()
        self.session = db_session
        self.metadata = db_metadata
        self.base = db_base

    def get_migrateversion(self):
        query = ("select migrate_version.version from migrate_version")
        version = self.db_session.execute(query)
        return version

class DB_dump:
    def __init__(self, user_source, host_source, port_source, password_source,
                 name_source, user_target, host_target, port_target,
                 password_target, name_target, file_path):
        self.user_source = " -u " + user_source
        self.host_source = " --host " + host_source
        self.port_source = " --port " + port_source
        self.password_source = " -p" + password_source
        self.name_source = " " + name_source
        self.user_target = " -u " + user_target
        self.host_target = " --host " + host_target
        self.port_target = " --port " + port_target
        self.password_target = " -p" + password_target
        self.name_target = " " + name_target
        self.file_path = " " + file_path

    def download(self):
        print ("[INFO] download DB")
        cmd = "mysqldump --single-transaction" + self.user_source + \
              self.host_source + self.port_source + self.password_source + \
              self.name_source + " > " + self.file_path
        cmd_out = "mysqldump --single-transaction" + self.user_source + \
              self.host_source + self.port_source + " -pXXX" + \
              self.name_source + " > " + self.file_path
        print ("[INFO] CMD: %s" % cmd_out)
        if os.system(cmd) != 0:
            print ("[ERROR] Cannot download DB - %s" % self.name_source)
            sys.exit(1)
        else:
            stat = os.stat('db.sql')
            print ("[INFO] DB file size: %d MB" % (stat.st_size / 1048576))

    def upload(self):
        print ("[INFO] upload DB")
        cmd = "mysql " + self.user_target + self.host_target + \
              self.port_target + self.password_target + \
              self.name_target + " < " + self.file_path
        cmd_out = "mysql " + self.user_target + self.host_target + \
              self.port_target + " -pXXX" + \
              self.name_target + " < " + self.file_path
        print ("[INFO] CMD: %s" % cmd_out)
        if os.system(cmd) != 0:
            print ("[ERROR] Cannot upload DB")
            sys.exit(1)

    def sync(self):
        print ("[INFO] db sync")
        cmd = "nova-manage --config-file nova.conf db sync"
        if os.system(cmd) != 0 :
            print ("[ERROR] Cannot sync DB")
            sys.exit(1)

    def version(self):
        print ("[INFO] db version")
        cmd = "nova-manage --config-file nova.conf db version"
        out = subprocess.check_output(cmd, shell=True)
        print out

def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
        default='config.conf',
        help='configuration file')
    return parser.parse_args()

def get_target_endpoint(config_file):
    print ("[INFO] extracting target endpoints")
    parser = ConfigParser.SafeConfigParser()
    parser.read(config_file)
    endpoints = {}
    try:
        endpoints['host'] = parser.get('target', 'db_location')
        endpoints['port'] = parser.get('target', 'db_port')
        endpoints['user'] = parser.get('target', 'user')
        endpoints['password'] = parser.get('target', 'password')
        endpoints['database'] = parser.get('target', 'database')
    except :
        print ("[ERROR] Can't get cell endpoints")
        sys.exit(1)
    return endpoints

def get_cells(config_file):
    parser = ConfigParser.SafeConfigParser()
    parser.read(config_file)
    cells = [cell for cell in parser.sections() if "cell_" in cell]
    return cells

def get_cell_endpoint(cell, config_file):
    print ("[INFO] extracting cell endpoints - %s" % cell)
    parser = ConfigParser.SafeConfigParser()
    parser.read(config_file)
    endpoints = {}
    try:
        endpoints['host'] = parser.get(cell, 'db_location')
        endpoints['port'] = parser.get(cell, 'db_port')
        endpoints['user'] = parser.get(cell, 'user')
        endpoints['password'] = parser.get(cell, 'password')
        endpoints['database'] = parser.get(cell, 'database')
    except:
        print ("[ERROR] Can't get cell endpoints - %s" % cell)
        sys.exit(1)
    return endpoints

def main():
    args = parse_cmdline_args()

    try:
        args = parse_cmdline_args()
    except Exception as e:
        print ("Wrong command line arguments")

    target_endpoint = get_target_endpoint(args.config)

    target_db_url = 'mysql://{user}:{password}@{location}:{port}/{database}'.format(
        user = target_endpoint['user'],
        password = target_endpoint['password'],
        location = target_endpoint['host'],
        port = target_endpoint['port'],
        database = target_endpoint['database'])

    db_query = DB_query(target_db_url)

    print "\n---\n"
    cells = get_cells(args.config)
    for cell in cells:

        source_endpoint = get_cell_endpoint(cell, args.config)

        db_dump = DB_dump(source_endpoint['user'],
                          source_endpoint['host'],
                          source_endpoint['port'],
                          source_endpoint['password'],
                          source_endpoint['database'],
                          target_endpoint['user'],
                          target_endpoint['host'],
                          target_endpoint['port'],
                          target_endpoint['password'],
                          target_endpoint['database'],
                          'db.sql'
                         )

        print ("[INFO] %s" % datetime.datetime.now())
        db_dump.download()
        print ("[INFO] %s" % datetime.datetime.now())
        db_dump.upload()
        print ("[INFO] %s" % datetime.datetime.now())
        db_dump.version()
        print ("[INFO] %s" % datetime.datetime.now())
        db_dump.sync()
        print ("[INFO] %s" % datetime.datetime.now())
        db_dump.version()
        print "\n---\n"

if __name__ == "__main__":
    main()
