import time

from flask import Flask, render_template, request, make_response, url_for, session
import json
import jsonrpc
import jsonrpclib
import random
import urllib.request
import getpass
import http
import requests
from pprint import pprint
import logging
import zipfile
import socket
import os
import xmlrpc.client
import base64

dir_path = os.path.dirname(os.path.realpath(__file__))

server_url  ='http://localhost:8090'
db_name = 'yuju'
username = 'admin'
password = 'odoo'

print('Conectando API Odoo...........')
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(server_url))
uid = common.authenticate(db_name, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(server_url))
print(common)
# Define the path to the folder containing the invoices
invoices_folder = dir_path + '/xml/'
#ID of the account.move record you want to add attachments to
move_id = 354876
#Location of the xml files
with open(dir_path + '/xml/797CE8FA-5B55-43DA-A15D-4A1C8008536E@1000000000XX0.xml', 'rb') as file:
    xml_data = file.read()

xml_base64 = base64.b64encode(xml_data).decode('utf-8')

attachment_data = {
    'name': '797CE8FA-5B55-43DA-A15D-4A1C8008536E@1000000000XX0.xml',
    'datas': xml_base64,
    'res_model': 'account.move',
    'res_id': move_id,
}
#Creation of attachment line
attachment_ids = models.execute_kw(db_name, uid, password, 'ir.attachment', 'create', [attachment_data])
print('')
time.sleep(3)
#Search current attachment id
fields = ['name', 'like', '797CE8FA-5B55-43DA-A15D-4A1C8008536E@1000000000XX0']
attachment = models.execute_kw(db_name, uid, password, 'ir.attachment', 'search_read',
                               [[fields]],
                               {'fields':
                                    ['id',
                                     ]})
attachment_id = attachment[-1]['id']
#Table values edi_document
#edi_format_id: 2 = CFDI 4.0
values = [{
            'move_id': move_id,
            'edi_format_id': 2,
            'attachment_id': attachment_id,
            'state': 'sent',
            'create_uid': 1,
            'write_uid': 2,
        }]
#The record in the table edi_document related to the invoice is created
edi_document = models.execute_kw(db_name, uid, password, 'account.edi.document', 'create', values)
print('Values: ',values)
print('Registro account.edi.document creado')
#Invoice status is updated to posted
upd_invoice_state = models.execute_kw(db_name, uid, password, 'account.move', 'write', [[move_id],{'state': 'posted'}])
print('Se cambia de estatus la factura: ', move_id)