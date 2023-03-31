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

logging.basicConfig(format='%(asctime)s|%(name)s|%(levelname)s|%(message)s', datefmt='%Y-%d-%m %I:%M:%S %p',level=logging.INFO)

dir_path = os.path.dirname(os.path.realpath(__file__))

server_url  ='http://localhost:8090'
db_name = 'yuju'
username = 'admin'
password = 'odoo'

print('Conectando API')
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(server_url))
uid = common.authenticate(db_name, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(server_url))
print('Conectado...........')
move_id = 346475  # replace with the ID of the account.move record you want to add attachments to
move = models.execute_kw(db_name, uid, password, 'account.move', 'read', [move_id], {'fields': ['name', 'partner_id']})
for each in move:
    name = str(each['name'])
    inv_name_xml = 'INV-' + name.replace('/', '') + '-MX-Invoice-4.0.xml'

with open(dir_path + '/xml/' + inv_name_xml, 'rb') as file:
    xml_data = file.read()

xml_base64 = base64.b64encode(xml_data).decode('utf-8')

attachment_data = {
    'name': inv_name_xml,
    'datas': xml_base64,
    #'datas_fname': 'M2211401.xml',
    'res_model': 'account.move',
    'res_id': move_id,
}

attachment_ids = models.execute_kw(db_name, uid, password, 'ir.attachment', 'create', [attachment_data])

print('Se adjunta archivo ', inv_name_xml, ' en la factura ', name)
print('')
time.sleep(3)

fields = ['name', 'like', str(inv_name_xml)]
attachment = models.execute_kw(db_name, uid, password, 'ir.attachment', 'search_read',
                               [[fields]],
                               {'fields':
                                    ['id',
                                     ]})
attachment_id = attachment[-1]['id']
edi_document = models.execute_kw(db_name, uid, password, 'account.edi.document', 'search_read', [[['move_id', '=', move_id]]],{'fields':['id']})
edi_document_id = edi_document[-1]['id']

upd_edi_document = models.execute_kw(db_name, uid, password, 'account.edi.document', 'write',[[int(edi_document_id)],{'attachment_id': int(attachment_id)}])

print('Segundo ID tabla edi.document:', edi_document_id)
print('Success')