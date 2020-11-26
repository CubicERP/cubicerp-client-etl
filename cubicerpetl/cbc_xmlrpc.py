#! /usr/bin/env python
# -*- encoding: utf-8 -*-
##############################################################################
#
# Copyright (C) 2014 Cubic ERP S.A.C (<http://cubicerp.com>).
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
##############################################################################

import openerplib
import odoolib
import sys
if sys.version > '3':
    from configparser import SafeConfigParser
else:
    from ConfigParser import SafeConfigParser
import os

def get_connection(database, hostname=None, port=None, login=None, password=None):
    parser = SafeConfigParser()
    pwd = os.environ.get('ETL_INI_CONFIG_PWD', False) or os.environ['PWD']
    filename = os.path.join(pwd, 'config', 'etl.ini')
    if not os.path.exists(filename):
        filename = os.path.join(os.path.dirname(__file__), 'config.ini')
    parser.read(filename)
    if hostname is None:
        hostname = parser.get(database, 'host')
    if port is None:
        port = parser.getint(database, 'port')
    if login is None:
        login = parser.get(database, 'username')
    if password is None:
        password = parser.get(database, 'password')

    return openerplib.get_connection(hostname=hostname, port=port, database=database, login=login, password=password)

def connection(database, hostname=None, port=None, login=None, password=None):
    parser = SafeConfigParser()
    pwd = os.environ.get('ETL_INI_CONFIG_PWD', False) or os.environ['PWD']
    filename = os.path.join(pwd, 'config', 'etl.ini')
    if not os.path.exists(filename):
        filename = os.path.join(os.path.dirname(__file__), 'config.ini')
    parser.read(filename)
    if hostname is None:
        hostname = parser.get(database, 'host')
    if port is None:
        port = parser.getint(database, 'port')
    if login is None:
        login = parser.get(database, 'username')
    if password is None:
        password = parser.get(database, 'password')

    return odoolib.get_connection(hostname=hostname, port=port, database=database, login=login, password=password)
