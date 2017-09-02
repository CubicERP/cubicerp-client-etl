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
import sys
from cubicerpetl import oer_etl

def run(hostname,port,database,login,password,log_print=True):
    oer_local = openerplib.get_connection(hostname=hostname,
                                     port=port, database=database,
                                     login=login, password=password)
    etl = oer_etl(oer_local, log_print=log_print)
    for job in etl.get_jobs():
        if etl.get_job_state(job['id']) != 'ready':
            continue
        oer_local.get_model('etl.job').action_start([job['id']])
        for row in etl.get_rows(job['id']):
            new_id = etl.create(job['id'], etl.get_values(job['id'],row), pk=row.get('pk',False))
        oer_local.get_model('etl.job').action_done([job['id']]) 
    print "Finish etl_cron"