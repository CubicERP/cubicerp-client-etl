
The OpenERP ETL Client Library is a Python library to extract transform and load dato from any data
source to OpenERP Server in an user-friendly way. To use this library you need install etl community
module on a OpenERP instance.
        
The OpenERP ETL Client Library is officially supported by Cubic ERP S.A.C.

To consult the source code repository, report bugs or ask questions, see the Public Branch of Cubic ERP:

https://github.com/CubicERP

To hire support contract and profesional services contact to us at info@cubicerp.com

OpenERP ETL Client Library Guide
--------------------------------
    
First install the library: ::

    sudo easy_install openerp-client-etl

Now you can run the ready jobs defined on OpenERP using the following script: ::

    import openerpetl

    openerpetl.run(hostname='localhost', port=8069, database='test', login='admin', password='admin')
    # will print "Finish etl_cron" when finish the jobs execution

This script will be used on a programed cron task execution or as comand line script in order to ensure
the correct execution of ETL jobs defined on OpenERP GUI.

Compatibility
-------------


- 1.1

 - XML-RPC: OpenERP version 6.1 and superior

 - JSON-RPC: OpenERP version 8.0 (upcoming) and superior


Changelog
---------

- 1.1.1:

 - Updated documentation