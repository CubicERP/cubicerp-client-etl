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

import logging
import decimal
import time
import re
import sys, traceback
import importlib
import os
import csv
if sys.version > '3':
    from io import StringIO
else:
    from StringIO import StringIO
import base64

_logger = logging.getLogger(__name__)


class cbc_etl(object):
    
    local = None
    __jobs = {}
    __servers = {}
    __resources = {}
    __transforms = {}
    __connections = {}
    __value_mapping = {}
    __resolve_xml_id = {}
    log_print = False
    
    def __init__(self, cbc_local, log_print=False):
        self.local = cbc_local
        self.invalidate_cache()
        self.log_print = log_print

    def invalidate_cache(self):
        self.__jobs = {}
        self.__servers = {}
        self.__resources = {}
        self.__transforms = {}
        self.__connections = {}
        self.__value_mapping = {}
        self.__resolve_xml_id = {}

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
        if self.__jobs.get(job_id):
            res = self.__jobs[job_id]
        else:
            job_obj = self.local.get_model('etl.job')
            res = job_obj.read([job_id])[0]
            self.__jobs[job_id] = res
        return res
    
    def get_job_state(self, job_id):
        job_obj = self.local.get_model('etl.job')
        return job_obj.read([job_id])[0]['state']
    
    def get_resource(self, resource_id):
        if self.__resources.get(resource_id):
            res = self.__resources[resource_id]
        else:
            resource_obj = self.local.get_model('etl.resource')
            res = resource_obj.read([resource_id])[0]

            column_obj = self.local.get_model('etl.resource.column')
            res['f_columns'] = []
            for f_column_id in res['f_column_ids']:
                column = column_obj.read([f_column_id])[0]
                res['f_columns'].append(column)

            action_obj = self.local.get_model('etl.resource.action')
            res['prev_actions'] = []
            for prev_action_id in res['prev_action_ids']:
                action = action_obj.read([prev_action_id])[0]
                res['prev_actions'].append(action)
            res['post_actions'] = []
            for post_action_id in res['post_action_ids']:
                action = action_obj.read([post_action_id])[0]
                res['post_actions'].append(action)

            field_obj = self.local.get_model('etl.field')
            res['rpc_fields'] = []
            for rpc_field_id in res['rpc_field_ids']:
                field = field_obj.read([rpc_field_id])[0]
                res['rpc_fields'].append(field)

            self.__resources[resource_id] = res
        return res

    def get_transform(self, transform_id):
        if self.__transforms.get(transform_id):
            res = self.__transforms[transform_id]
        else:
            transform_obj = self.local.get_model('etl.transform')
            field_obj = self.local.get_model('etl.field')
            res = transform_obj.read([transform_id])[0]
            res['fields'] = []
            for field_id in res['field_ids']:
                field = field_obj.read([field_id])[0]
                res['fields'].append(field)
            self.__transforms[transform_id] = res
        return res

    def get_server(self, server_id):
        if self.__servers.get(server_id):
            res = self.__servers[server_id]
        else:
            server_obj = self.local.get_model('etl.server')
            res = server_obj.read([server_id])[0]
            self.__servers[server_id] = res
        return res
    
    def get_connection(self, server_id):
        conn = False
        server = self.get_server(server_id)
        if server['etl_type'] == 'rpc':
            if self.__connections.get(server_id):
                conn = self.__connections[server_id]
            else:
                lib = importlib.import_module(server.get('driver') or server['rpc_protocol'] or 'cubicerpetl.cbc_xmlrpc')
                conn = lib.get_connection(hostname=server['rpc_host'], port=server['rpc_port'], database=server['rpc_database'],
                                          login=server['login'], password=server['password'])
                self.__connections[server_id] = conn
        elif server['etl_type'] == 'db':
            lib = importlib.import_module(server.get('driver') or server['db_type'] or 'psycopg2')
            conn = lib.connect(server.get('str_connection', "'dbname'=%s"%self.local.database))
        elif server['etl_type'] == 'fs':
            if server['fs_protocol'] == 'file':
                lib = server.get('driver') and importlib.import_module(server['driver']) or cbc_file
                conn = lib(server['fs_path'])
            elif server['fs_protocol'] == 'ftp':
                lib = importlib.import_module(server.get('driver') or 'ftplib.FTP')
                conn = lib(server['fs_host'], server['login'], server['password'])
        _logger.debug('Server Connection %s',conn)
        return conn
    
    def do_extract(self, resource_id, server_id=None, job_id=None, localdict={}, context={}):
        if job_id:
            job = self.get_job(job_id)
            if not server_id:
                server_id = job['extract_server_id'] and job['extract_server_id'][0] or None
        conn = server_id and self.get_connection(server_id) or self.local
        server = server_id and self.get_server(server_id) or {'encoding': False, 'etl_type': 'rpc'}
        resource = self.get_resource(resource_id)
        query_encoding = resource['encoding'] or server['encoding']
        rows = []
        if resource['etl_type'] == 'fs':
            fl = StringIO()
            if job['type'] == 'online':
                if job_id and job['input_file']:
                    fl = StringIO(base64.b64decode(job['input_file']).decode('utf-8'))
                elif server['etl_type'] == 'fs':
                    fl = conn.open(resource['f_filename'])
            else:
                if server['etl_type'] == 'fs':
                    fl = conn.open(resource['f_filename'])
                elif job_id and job['input_file']:
                    fl = StringIO(base64.b64decode(job['input_file']).decode('utf-8'))

            cols = [c['field_name'] or c['name'] for c in resource['f_columns']]

            header_cols = []
            if resource['f_header_id']:
                header = self.get_resource(resource['f_header_id'][0])
                header_cols = [c['field_name'] or c['name'] for c in header['f_columns']]

            footer_cols = []
            if resource['f_footer_id']:
                footer = self.get_resource(resource['f_footer_id'][0])
                footer_cols = [c['field_name'] or c['name'] for c in footer['f_columns']]

            if resource['f_type'] == 'txt':
                fp = []
                for line in fl:
                    fp.append(line)
                widths = [slice(c['txt_position'] - 1, c['txt_position'] + c['txt_lenght'] -1) for c in resource['f_columns']]
                hf = {}
                last = len(fp) - 1
                if header_cols and fp:
                    header_w = [slice(c['txt_position'] - 1, c['txt_position'] + c['txt_lenght'] -1) for c in header['f_columns']]
                    hf.update(dict([(header_cols[i], fp[0][w]) for i, w in enumerate(header_w)]))
                if footer_cols and fp:
                    footer_w = [slice(c['txt_position'] - 1, c['txt_position'] + c['txt_lenght'] -1) for c in footer['f_columns']]
                    hf.update(dict([(footer_cols[i], fp[last][w]) for i, w in enumerate(footer_w)]))
                for i,line in enumerate(fp):
                    if i == 0 and header_cols:
                        continue
                    if i == last and footer_cols:
                        continue
                    row = dict([(cols[i], line[w]) for i, w in enumerate(widths)])
                    row.update(hf)
                    rows.append(row)
            elif resource['f_type'] == 'csv':
                reader = csv.DictReader(fl,fieldnames=cols or None, delimiter=resource['txt_separator'] or ',',
                                      quotechar=resource['txt_quote'] or '"')
                rows = [r for r in reader]
            fl.close()
        elif resource['etl_type'] == 'db':
            cr = conn.cursor()
            if resource['query_begin']:
                query_begin = resource['query_begin'] % localdict
                # if type(query_begin) is unicode and query_encoding:
                #     query_begin = query_begin.encode(query_encoding)
                cr.execute(query_begin)
                if resource.get('query_begin_delay'):
                    self.log('Query Begin is executing, time to waiting %d sec. (%s)' % (
                        resource['query_begin_delay'], time.strftime('%Y-%m-%d %H:%M.%S')), server_id=server_id, resource_id=resource_id)
                    time.sleep(resource['query_begin_delay'])
            query = resource['query'] % localdict
            # if type(query) is unicode and query_encoding:
            #     query = query.encode(query_encoding)
            cr.execute(query)
            rows = cr.fetchall()
            row_description = cr.description
            if resource['query_end']:
                query_end = resource['query_end'] % localdict
                # if type(query_end) is unicode and query_encoding:
                #     query_end = query_end.encode(query_encoding)
                cr.execute(query_end)
            rows = [dict([(type(col) is tuple and col[0] or col.name, r[i]) for i, col in enumerate(row_description)])
                    for r in rows]
            cr.close()
            conn.close()
        elif resource['etl_type'] == 'rpc':
            if resource['rpc_python']:
                localdict = {'rows': [], 'conn': conn, 'context': context}
                exec(resource['rpc_python_code'], localdict)
                self.to_log(job_id, server_id, resource_id, localdict.get('to_log'))
                rows = localdict.get('rows', [])
            else:
                model_obj = self.local.get_model(resource['rpc_model_name'])
                model_ids = model_obj.search(eval(resource['rpc_domain']))
                rows = model_obj.read(model_ids,[r['field_name'] for r in resource['rpc_fields']])

        default_value = {}
        if resource['row_default_value']:
            default_value = eval(resource['row_default_value']%localdict)
        res = []
        for r in rows:
            d = default_value.copy()
            if sys.version > '3':
                for x,y in r.items():
                    if y is None:
                        y = False
                    if type(y) is decimal.Decimal:
                        y = float(y)
                    elif type(y) is str and query_encoding:
                        y = y.decode(query_encoding)
                    d[x] = y
            else:
                for x,y in r.iteritems():
                    if y is None:
                        y = False
                    if type(y) is decimal.Decimal:
                        y = float(y)
                    elif type(y) is str and query_encoding:
                        y = y.decode(query_encoding)
                    d[x] = y
            res.append(d)
        return res

    def do_transform(self, rows, transform_id, job_id):
        if not transform_id:
            return rows
        transform = self.get_transform(transform_id)
        job = self.get_job(job_id)
        server_id = job['load_server_id'] and job['load_server_id'][0] or False
        conn2 = server_id and self.get_connection(server_id) or self.local
        server_id = job['extract_server_id'] and job['extract_server_id'][0] or False
        conn1 = server_id and self.get_connection(server_id) or self.local
        ress = []
        for row in rows:
            res = {}
            localdict = {'row': row, 'rows': rows, 'res': res, 'conn1': conn1, 'conn2': conn2}
            if transform['prev_python']:
                exec(transform['prev_python_code'], localdict)
                self.to_log(job_id, server_id, False, localdict.get('to_log'))
                if localdict.get('break_on', False):
                    break
                if localdict.get('continue_on', False):
                    continue
            res.update(self.get_values(job_id, row))
            if transform['post_python']:
                exec (transform['post_python_code'], localdict)
                self.to_log(job_id, server_id, False, localdict.get('to_log'))
                if localdict.get('break_on', False):
                    break
                if localdict.get('continue_on', False):
                    continue
            if res:
                ress.append(res)
        return ress

    def do_load(self, rows, job_id, context={}):
        job = self.get_job(job_id)
        resource_id = job['load_resource_id'][0]
        server_id = job['load_server_id'] and job['load_server_id'][0] or False
        transform_id = job['transform_id'] and job['transform_id'][0] or False
        conn = server_id and self.get_connection(server_id) or self.local
        server = server_id and self.get_server(server_id) or {'encoding': False, 'etl_type': 'rpc'}
        resource = self.get_resource(resource_id)
        query_encoding = resource['encoding'] or server['encoding']

        if transform_id and server['etl_type'] == 'rpc' and rows:
            transform = self.get_transform(transform_id)
            r_fields = ['id'] + [field['field_name'] for field in transform['fields'] if field['field_type'] == 'many2one']
            for row in rows:
                for field in r_fields:
                    val = row.get(field, False)
                    if not (type(val) is str):
                        continue
                    if re.match(r'^[a-zA-Z_]+.[a-zA-Z_]+$', val):
                        val = self.get_resolve_xml_id(val, server_id=server_id)
                        row[field] = type(val) is int and val or False
                    elif re.match(r"""^\(\s*('|")[a-zA-Z_.]+('|")\s*,\s*\[.*\]\s*\)$""", val):
                        obj, dom = eval(val)
                        val = conn.get_model(obj).search(dom)
                        row[field] = val and val[0] or False
                    else:
                        row[field] = False
        vals = []
        if resource['etl_type'] == 'fs':
            if resource['f_type'] == 'txt':
                if resource['f_header_id']:
                    vals += self.get_txt_lines(rows, resource['f_header_id'][0])
                vals += self.get_txt_lines(rows, resource_id)
                if resource['f_footer_id']:
                    vals += self.get_txt_lines(rows, resource['f_footer_id'][0])

                if server['etl_type'] == 'fs':
                    fl = conn.open(resource['f_filename'], "w")
                    for val in vals:
                        fl.write(query_encoding and ('%s\r\n' % val).encode(query_encoding) or '%s\r\n' % val)
                    fl.close()
            elif resource['f_type'] == 'csv':
                cols = [col['field_name'] or col['name'] for col in resource['f_columns']]
                for row in rows:
                    val = {}
                    for col in resource['f_columns']:
                        val[col['field_name'] or col['name']] = col['forced_value'] or row.get(col['field_name'] or col['name'], '')
                    vals.append(val)
                buf = StringIO()
                writer = csv.DictWriter(buf, cols, delimiter=resource['txt_separator'] or ',', quotechar=resource['txt_quote'] or '"')
                if resource['txt_header']:
                    writer.writeheader()
                writer.writerows(vals)
                if server['etl_type'] == 'fs' and cols:
                    fl = conn.open(resource['f_filename'], "w")
                    fl.write(buf.getvalue())
                    fl.close()
                buf.seek(0)
                vals = [b and b[:-2] or '' for b in buf.readlines()]
                buf.close()

        elif resource['etl_type'] == 'db':
            cr = conn.cursor()
            for row in rows:
                if resource['query_begin']:
                    query_begin = resource['query_begin'] % row
                    # if type(query_begin) is unicode and query_encoding:
                    #     query_begin = query_begin.encode(query_encoding)
                    cr.execute(query_begin)
                    if resource.get('query_begin_delay'):
                        self.log('Query Begin is executing, time to waiting %d sec. (%s)' % (
                            resource['query_begin_delay'], time.strftime('%Y-%m-%d %H:%M.%S')), server_id=server_id,
                                 resource_id=resource_id)
                        time.sleep(resource['query_begin_delay'])
                query = resource['query'] % row
                # if type(query) is unicode and query_encoding:
                #     query = query.encode(query_encoding)
                cr.execute(query)
                val = cr.fetchall()
                row_description = cr.description
                if resource['query_end']:
                    query_end = resource['query_end'] % row
                    # if type(query_end) is unicode and query_encoding:
                    #     query_end = query_end.encode(query_encoding)
                    cr.execute(query_end)
                vals += [dict([(type(col) is tuple and col[0] or col.name, r[i]) for i, col in enumerate(row_description)]) for r in val]
            cr.close()
            conn.close()

        elif resource['etl_type'] == 'rpc':
            if resource['rpc_python']:
                localdict =  {'rows':rows, 'conn': conn, 'context': context}
                for row in rows:
                    localdict['row'] = row
                    exec(resource['rpc_python_code'], localdict)
                    self.to_log(job_id, server_id, resource_id, localdict.get('to_log'))
                    if localdict.get('break_on', False):
                        break
                return rows
            model = conn.get_model(resource['rpc_model_name'])
            transform = transform_id and self.get_transform(transform_id) or {'reprocess': 'insert'}
            if transform['reprocess'] == 'delete':
                model.unlink([r['id'] for r in rows if r.get('id', False)])
            cols = [f['field_name'] for f in resource['rpc_fields']]
            if not transform['reprocess'] or transform['reprocess'] in ['delete', 'insert']:
                for row in [r for r in rows if r.get('id', False) is not None]:
                    val = {}
                    for col in cols:
                        val[col] = row.get(col)
                    if val.get('id'):
                        del val['id']
                    vals .append(val)
                    self.create(job_id, val, pk=row.get('pk',False))
            if transform['reprocess'] in ['onlyupdate', 'update']:
                for row in [r for r in rows if r.get('id', False)]:
                    val = {}
                    for col in cols:
                        val[col] = row.get(col)
                    val_id = row['id']
                    if val.get('id'):
                        del val['id']
                    self.write(job_id, val_id, val, pk=row.get('pk', False))
                    val['id'] = row['id']
                    vals .append(val)
            if transform['reprocess'] in ['noupdate', 'update']:
                for row in [r for r in rows if r.get('id', False) is not None and not r.get('id', False)]:
                    val = {}
                    for col in cols:
                        val[col] = row.get(col)
                    if val.get('id'):
                        del val['id']
                    vals .append(val)
                    self.create(job_id, val, pk=row.get('pk',False))
        if transform_id and rows:
            transform = self.get_transform(transform_id)
            if transform.get('end_python'):
                server_id1 = job['extract_server_id'] and job['extract_server_id'][0] or False
                conn1 = server_id1 and self.get_connection(server_id1) or self.local
                localdict = {'rows': rows, 'conn1': conn1, 'conn2': conn}
                exec(transform['end_python_code'], localdict)
                self.to_log(job_id, server_id, resource_id, localdict.get('to_log'))
        return vals

    def get_txt_lines(self, rows, resource_id):
        lines = []
        resource = self.get_resource(resource_id)
        for row in rows:
            line = ""
            pos = 1
            for col in resource['f_columns']:
                line += "%*s" % (col['txt_position'] - pos, '')
                val = col['forced_value'] or row.get(col['field_name'] or col['name'], '')
                if col['txt_align'] == 'rjust':
                    line += str(val)[col['txt_lenght'] * -1:].rjust(col['txt_lenght'], col['txt_fill_char'] or ' ')
                elif col['txt_align'] == 'center':
                    line += str(val)[:col['txt_lenght']].center(col['txt_lenght'], col['txt_fill_char'] or ' ')
                else:
                    line += str(val)[:col['txt_lenght']].ljust(col['txt_lenght'], col['txt_fill_char'] or ' ')
                pos += (col['txt_lenght'] + col['txt_position'] - pos)
            lines.append(line)
        return lines
    
    def get_resolve_xml_id(self, xml_id, server_id=False):
        res = False
        if type(xml_id) is not str:
            return res
        mod_xml = xml_id.split('.')
        if len(mod_xml) == 2:
            key = "%s.%s" % (server_id, xml_id)
            if not self.__resolve_xml_id.get(key):
                model_data_obj = (server_id and self.get_connection(server_id) or self.local).get_model('etl.mapping')
                self.__resolve_xml_id[key] = model_data_obj.get_object_reference(mod_xml[0],mod_xml[1])[1]
                if not self.__resolve_xml_id[key]:
                    self.log('The XML_ID: %s not found on destinity server'%xml_id, server_id=server_id, level='warning')
            res = self.__resolve_xml_id.get(key)
        return res

    def get_resolve_name_search(self, field_relation, name_search, server_id=False):
        val_obj = (server_id and self.get_connection(server_id) or self.local).get_model(field_relation)
        val = False
        if name_search:
            val = val_obj.search(name_search)
        if val:
            val = val[0]
        else:
            self.log('The domain: %s %s not found on destinity server' % (field_relation, name_search), server_id=server_id, level='warning')
        return val
    
    def get_value_mapping(self, mapping_id, val):
        if not self.__value_mapping.get(mapping_id):
            mapping_obj = self.local.get_model('etl.mapping')
            line_obj = self.local.get_model('etl.mapping.line')
            mapping = mapping_obj.read([mapping_id])[0]
            self.__value_mapping[mapping_id] = {'__return_null_value__': mapping.get('return_null',True)}
            for line in line_obj.read(mapping['line_ids']):
                line_value = False
                if line.get('map_ref'):
                    line_value = int(line['map_ref'].split(',')[1])
                elif line.get('map_id'):
                    line_value = line['map_id']
                elif line.get('map_xml_id'):
                    line_value = line['map_xml_id']
                elif line.get('map_char'):
                    line_value = line['map_char']
                if line.get('is_default',False):
                    self.__value_mapping[mapping_id]['__is_default_value__'] = line_value
                self.__value_mapping[mapping_id][line['name']] = line_value
        if not val and self.__value_mapping[mapping_id]['__return_null_value__']:
            return val
        if self.__value_mapping[mapping_id].get('__is_default_value__') and not self.__value_mapping[mapping_id].get(val):
            val = self.__value_mapping[mapping_id].get('__is_default_value__')
        else:
            val = self.__value_mapping[mapping_id].get(val,val)
        return val
        
    def get_values(self, job_id, row):
        res = {}
        job = self.get_job(job_id)
        if not job['transform_id']:
            return row
        transform_id = job['transform_id'][0]
        transform = self.get_transform(transform_id)
        if row.get('pk'):
            res['pk'] = row['pk']
        if job_id and row.get('pk') and transform['reprocess'] in ('noupdate', 'update', 'onlyupdate', 'delete'):
            model_ids = []
            res['id'] = False
            for log in self.get_logs(job_id, row.get('pk'), level='info'):
                if log['model_id']:
                    model_ids.append(log['model_id'])
            if model_ids and transform['reprocess'] == 'noupdate':
                res['id'] = None
            elif model_ids and transform['reprocess'] in ['update','delete']:
                res['id'] = model_ids[0]
            elif not model_ids and transform['reprocess'] == 'onlyupdate':
                res['id'] = None

        for map in transform['fields']:
            val = eval(map['value'],row)
            if map['mapping_id']:
                val = self.get_value_mapping(map['mapping_id'][0],val)
            row['__value_mapping__'] = val
            if map['field_type'] in ('char','text','selection'):
                if val and type(val) is not str and transform['encoding']:
                    val = val.decode(transform['encoding'])
            elif map['field_type'] == 'date':
                if val and type(val) is not str:
                    val = val.strftime('%Y-%m-%d')
            elif map['field_type'] == 'datetime':
                if val and type(val) is not str:
                    val = val.strftime('%Y-%m-%d %H:%M:%S')
            elif map['search_null'] and (not val):
                pass
            elif map['field_type'] == 'many2one' or map['field_name'] == 'id':
                if type(val) is not int:
                    if not re.match(r'^[a-zA-Z_]+.[a-zA-Z_]+$',val):
                        if map['name_search']:
                            dom = eval(map['name_search'],row)
                        else:
                            dom = [('name','=',val)]
                        val = "('%s',%s)" % (map['field_relation'], str(dom))
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
            self.log("%s Not Found on source server %s"%(value,self.get_job(job_id)['extract_server_id']), level='error',
                     job_id=job_id)
        return value
    
    def write(self, job_id, _id, values, pk=False, model=None, server_id=False):
        job = self.get_job(job_id)
        resource_id = job['load_resource_id'][0]
        server_id = server_id or (job['load_server_id'] and job['load_server_id'][0] or False)
        conn = server_id and self.get_connection(server_id) or self.local
        resource = self.get_resource(resource_id)
        model_name = model or resource['rpc_model_name']
        load_model = conn.get_model(model or resource['rpc_model_name'])

        res = False
        try:
            res = load_model.write([_id], values)
        except Exception as e:
            msg = '\n%s.write %s(%s,)%s'%(load_model.model_name,_id,str(values))
            msg += '\nUnexpected Error: \n%s\n\n%s'%(e.faultCode,e.faultString)
            self.log(msg, stack=sys.exc_info(), job_id=job['id'], level='error', pk=pk)
        else:
            self.log('Ok', job_id=job['id'], server_id=server_id, resource_id=resource_id, id=_id, pk=pk, model=model_name)
        return res

    def create(self, job_id, values, pk=False, model=None, server_id=False):
        job = self.get_job(job_id)
        resource_id = job['load_resource_id'][0]
        server_id = server_id or (job['load_server_id'] and job['load_server_id'][0] or False)
        conn = server_id and self.get_connection(server_id) or self.local
        resource = self.get_resource(resource_id)
        model_name = model or resource['rpc_model_name']
        load_model = conn.get_model(model_name)

        new_id = False
        try:
            new_id = load_model.create(values)
        except Exception as e:
            msg = '\n%s.create '%load_model.model_name + str(values)
            msg += '\nUnexpected Error: \n%s\n\n%s'%(e.faultCode,e.faultString)
            self.log(msg, stack=sys.exc_info(), job_id=job['id'], level='error', pk=pk)
        else:
            self.log('Ok', job_id=job['id'], server_id=server_id, resource_id=resource_id, id=new_id, pk=pk, model=model_name)
        return new_id

    def to_log(self,job_id, server_id, resource_id, to_log):
        res = False
        if to_log and type(to_log) is not dict:
            res = []
            for t in to_log:
                res = self.log(t.get('msg', '-'), job_id=job_id, server_id=server_id, resource_id=resource_id,
                         level=t.get('level', 'info'), id=t.get('model_id', t.get('id')), pk=t.get('pk', False),
                         model=t.get('model', False), log=t.get('log', False), check=t.get('check', False), amount=t.get('amount',0.0))
        elif to_log and type(to_log) is dict:
            res = self.log(to_log.get('msg', '-'), job_id=job_id, server_id=server_id, resource_id=resource_id,
                     level=to_log.get('level', 'info'), id=to_log.get('model_id', to_log.get('id')), pk=to_log.get('pk', False),
                     model=to_log.get('model', False), log=to_log.get('log', False), check=to_log.get('check', False), amount=to_log.get('amount',0.0))

        return res

    def log(self, msg, job_id=None, server_id=None, resource_id=None, level=None, id=None, pk=None, stack=None, model=None, log=None, check=False, amount=0.0):
        msg = msg.replace('\\\\n','\\n')
        if self.log_print: to_print = "Job: %s - Message:%s"%(job_id,msg.replace('\\\\n','\\n'))
        log_obj = self.local.get_model('etl.log')
        vals = {'message': msg, 'check': check, 'amount': amount}
        if job_id:
            vals['job_id'] = job_id
        if server_id:
            vals['server_id'] = server_id
        if resource_id:
            vals['resource_id'] = resource_id
        if model:
            vals['model'] = model
        if level:
            vals['level'] =  level
        if log:
            vals['log'] = log
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
        if self.log_print: _logger.info(to_print.encode('ascii','replace'))
        return log_obj.create(vals)


class cbc_file(object):

    path = None

    def __init__(self, path):
        self.path = path

    def open(self, filename, mode="r"):
        filename = os.path.join(self.path, filename)
        f = open(filename, mode)
        return f
