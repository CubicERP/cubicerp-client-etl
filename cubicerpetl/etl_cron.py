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

from .cubicerpetl import cbc_etl
from . import cbc_xmlrpc
import logging
_logger = logging.getLogger(__name__)

def run(database, host=None, port=None, username=None, password=None, log_print=True, job_id=False):
    cbc_local = cbc_xmlrpc.get_connection(hostname=host, port=port, database=database,
                                          login=username, password=password)
    etl = cbc_etl(cbc_local, log_print=log_print)
    for job in etl.get_jobs():
        if job_id:
            if job['id'] != job_id:
                continue
        elif etl.get_job_state(job['id']) != 'ready':
            continue
        cbc_local.get_model('etl.job').action_start([job['id']])
        for row in etl.get_rows(job['id']):
            new_id = etl.create(job['id'], etl.get_values(job['id'],row), pk=row.get('pk',False))
        cbc_local.get_model('etl.job').action_done([job['id']]) 
    _logger.info("Finish etl_cron") 