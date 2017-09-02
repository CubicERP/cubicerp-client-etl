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

import pyodbc
import openerplib
import psycopg2
import logging
import decimal
import time
import sys, traceback
_logger = logging.getLogger(__name__)

class oer_etl(object):
    
    local = None
    __jobs = {}
    __servers = {}
    __connections = {}
    __value_mapping = {}
    __resolve_xml_id = {}
    log_print = False
    
    def __init__(self, oer_local, log_print=False):
        self.local = oer_local
        self.__jobs = {}
        self.__servers = {}
        self.__connections = {}
        self.__value_mapping = {}
        self.__resolve_xml_id = {}
        self.log_print = log_print
    
    def get_jobs(self):
        job_obj = self.local.get_model('etl.job')
        job_ids = job_obj.search([('state','=','ready'),('type','=','batch')])
        res = []
        for job_id in job_ids:
            job = self.get_job(job_id)
            self.__jobs[job_id] = job
            res.append(job)
        _logger.info('Geting %s Jobs',len(job_ids))
        return res
    
    def get_job(self, job_id):
        res = False
        if self.__jobs.has_key(job_id):
            res = self.__jobs[job_id]
        else:
            job_obj = self.local.get_model('etl.job')
            map_obj = self.local.get_model('etl.mapping.job')
            res = job_obj.read([job_id])[0]
            res['mappings'] = []
            for map_id in res['mapping_ids']:
                map = map_obj.read([map_id])[0]
                res['mappings'].append(map)
            self.__jobs[job_id] = res
        return res
    
    def get_job_state(self, job_id):
        job_obj = self.local.get_model('etl.job')
        return job_obj.read([job_id])[0]['state']
    
    def get_server(self, server_id):
        res = False
        if self.__servers.has_key(server_id):
            res = self.__servers[server_id]
        else:
            server_obj = self.local.get_model('etl.server')
            res = server_obj.read([server_id])[0]
            self.__servers[server_id] = res
        return res
    
    def get_connection(self, server_id):
        conn = False
        server = self.get_server(server_id)
        if server['type'] == 'xmlrpc':
            if self.__connections.has_key(server_id):
                conn = self.__connections[server_id]
            else:
                conn = openerplib.get_connection(hostname=server['host'],
                                             port=server['port'], database=server['database'],
                                             login=server['login'], password=server['password'])
                self.__connections[server_id] = conn
        elif server['type'] == 'odbc':
            conn = pyodbc.connect(server['str_connection'])
        elif server['type'] == 'postgresql':
            conn = psycopg2.connect(server['str_connection'])
        _logger.debug('Server Connection %s',conn)
        return conn
    
    def get_rows(self, job_id, localdict={}):
        job = self.get_job(job_id)
        conn = self.get_connection(job['src_server_id'][0])
        cr = conn.cursor()
        if job['query_begin']:
            query_begin = job['query_begin']%localdict
            if type(query_begin) is unicode and job['query_encoding']:
                query_begin = query_begin.encode(job['query_encoding'])
            cr.execute(query_begin)
            if job.get('query_begin_delay'):
                self.log(job['id'],'Query Begin is executing, time to waiting %d sec. (%s)'%(job['query_begin_delay'],time.strftime('%Y-%m-%d %H:%M.%S')))
                time.sleep(job['query_begin_delay'])
        query = job['query']%localdict
        if type(query) is unicode and job['query_encoding']:
            query = query.encode(job['query_encoding'])
        cr.execute(query)
        rows = cr.fetchall()
        row_description = cr.description
        if job['query_end']:
            query_end = job['query_end']%localdict
            if type(query_end) is unicode and job['query_encoding']:
                query_end = query_end.encode(job['query_encoding'])
            cr.execute(query_end)
        rows = [dict([(type(col) is tuple and col[0] or col.name,r[i]) for i,col in enumerate(row_description)]) for r in rows]
        default_value = {}
        if job['row_default_value']:
            default_value = eval(job['row_default_value']%localdict)
        res = []
        for r in rows:
            d = default_value.copy()
            for x,y in r.iteritems():
                if y is None:
                    y = False
                if type(y) is decimal.Decimal:
                    y = float(y)
                elif type(y) is str and job['query_encoding']:
                    y = y.decode(job['query_encoding'])
                d[x] = y
            res.append(d)
        cr.close()
        conn.close()
        return res
    
    def get_resolve_xml_id(self, job_id, xml_id):
        res = False
        if type(xml_id) is not str and type(xml_id) is not unicode:
            return res
        job = self.get_job(job_id)
        mod_xml = xml_id.split('.')
        if len(mod_xml) == 2:
            if not self.__resolve_xml_id.has_key(xml_id):
                model_data_obj = self.get_connection(job['dst_server_id'][0]).get_model('etl.mapping')
                self.__resolve_xml_id[xml_id] = model_data_obj.get_object_reference(mod_xml[0],mod_xml[1])[1]
                if not self.__resolve_xml_id[xml_id]:
                    self.log(job_id,'The XML_ID: %s not found on destinity server'%xml_id, level='warning')
            res = self.__resolve_xml_id.get(xml_id)
        return res
    
    def get_value_mapping(self, mapping_id, val, job_id):
        if not self.__value_mapping.has_key(mapping_id):
            mapping_obj = self.local.get_model('etl.mapping')
            line_obj = self.local.get_model('etl.mapping.line')
            mapping = mapping_obj.read([mapping_id])[0]
            job = self.get_job(job_id)
            self.__value_mapping[mapping_id] = {'__return_null_value__': mapping.get('return_null',True)}
            for line in line_obj.read(mapping['line_ids']):
                line_value = False
                if line.get('map_ref'):
                    line_value = int(line['map_ref'].split(',')[1])
                elif line.get('map_id'):
                    line_value = line['map_id']
                elif line.get('map_xml_id'):
                    line_value = self.get_resolve_xml_id(job_id, line['map_xml_id'])
                elif line.get('map_char'):
                    line_value = line['map_char']
                if line.get('is_default',False):
                    self.__value_mapping[mapping_id]['__is_default_value__'] = line_value
                self.__value_mapping[mapping_id][line['name']] = line_value
        if not val and self.__value_mapping[mapping_id]['__return_null_value__']:
            return val
        if self.__value_mapping[mapping_id].has_key('__is_default_value__') and not self.__value_mapping[mapping_id].has_key(val):
            val = self.__value_mapping[mapping_id].get('__is_default_value__')
        else:
            val = self.__value_mapping[mapping_id].get(val,val)
        return val
        
    def get_values(self, job_id, row):
        res = {}
        job = self.get_job(job_id)
        for map in job['mappings']:
            val = eval(map['value'],row)
            if map['mapping_id']:
                val = self.get_value_mapping(map['mapping_id'][0],val,job_id)
            row['__value_mapping__'] = val
            if map['field_type'] in ('char','text','selection'):
                if val and type(val) is not unicode and job['query_encoding']:
                    val = val.decode(job['query_encoding'])
            elif map['field_type'] == 'date':
                if val and type(val) not in (str, unicode):
                    val = val.strftime('%Y-%m-%d')
            elif map['field_type'] == 'datetime':
                if val and (type(val) is not str or type(val) is not unicode):
                    val = val.strftime('%Y-%m-%d %H:%M:%S')
            elif map['search_null'] and (not val):
                pass
            elif map['field_type'] == 'many2one':
                val_obj = self.get_connection(job['dst_server_id'][0]).get_model(map['field_relation'])
                if map['name_search']:
                    val_ids = val_obj.search(eval(map['name_search'],row))
                else:
                    val_ids = val_obj.search([('name','=',val)])
                if val_ids:
                    val = val_ids[0]
                if type(val) is not int:
                    if self.get_resolve_xml_id(job_id, val):
                        val = self.get_resolve_xml_id(job_id, val)
                    elif map['per_call_job_id']:
                        per_call_job = self.get_job(map['per_call_job_id'][0])
                        if per_call_job['state'] == 'ready':
                            val = self.create_from_value(map['per_call_job_id'][0], val, row)
                        else:
                            self.log(job_id, "The per call job %s can not called because is in %s state"%(per_call_job['name'],per_call_job['state']), level='warning', pk=row.get('pk',False))
            res[map['field_name']] = val
        return res
    
    def get_logs(self, job_id, pk=None, id=None, level=None):
        log_obj = self.local.get_model('etl.log')
        domain = [('job_id','=',job_id)]
        if pk:
            domain.append(('pk','=',pk))
        if id:
            domain.append(('model_id','=',id))
        if level:
            domain.append(('level','=',level))
        log_ids = log_obj.search(domain)
        return log_obj.read(log_ids) 
        
    def create_from_value(self, job_id, value, row):
        localdict = row.copy()
        localdict['etl_row_value'] = value
        rows = self.get_rows(job_id, localdict)
        if rows:
            value = self.create(job_id, self.get_values(job_id, rows[0]), pk=rows[0].get('pk',False))
        else:
            self.log(job_id, "%s Not Found on source server %s"%(value,self.get_job(job_id)['src_server_id']), level='error')
        return value
    
    def create(self, job_id, values, pk=False):
        job = self.get_job(job_id)
        oer = self.get_connection(job['dst_server_id'][0])
        load_model = oer.get_model(job['load_model'])
        if job['reprocess'] and pk:
            model_ids = []
            for log in self.get_logs(job_id,pk,level='info'):
                if log['model_id']:
                    model_ids.append(log['model_id'])
            if model_ids:
                model_ids = load_model.search([('id','in',model_ids)])
            if model_ids:
                return model_ids[0]
        new_id = False
        try:
            new_id = load_model.create(values)
        except Exception as e:
            msg = '\n%s.create '%load_model.model_name + str(values)
            msg += '\nUnexpected Error: \n%s\n\n%s'%(e.faultCode,e.faultString)
            self.log(job['id'], msg, stack=sys.exc_info(), level='error', pk=pk)
        else:
            self.log(job['id'],'Ok', id=new_id, pk=pk)
        return new_id
    
    def log(self, job_id, msg, level=None, id=None, pk=None, stack=None):
        msg = msg.replace('\\\\n','\\n')
        if self.log_print: to_print = "Job: %s - Message:%s"%(job_id,msg.replace('\\\\n','\\n'))
        log_obj = self.local.get_model('etl.log')
        job = self.get_job(job_id)
        vals = {'job_id': job_id,'log': msg, 'model': job['load_model']}
        if level:
            vals['level'] =  level
        if id:
            vals['model_id'] = id
            if self.log_print: to_print += " - ID:%s"%id
        if pk:
            vals['pk'] = pk
            if self.log_print: to_print += " - PK:%s"%pk 
        if stack:
            exc_type, exc_value, exc_traceback = stack
            stack = traceback.format_exception(exc_type, exc_value, exc_traceback)
            vals['traceback'] = ''.join(stack)
        if self.log_print: print to_print.encode('ascii','replace')
        return log_obj.create(vals)
