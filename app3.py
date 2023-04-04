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
print('')

#Conexion con API Google Drive
print('Conexion con la API de Google')
#//////
#//////
#//////
#//////
#//////
#//////
#//////
#//////
#//////

# Define the path to the folder containing the invoices
print('Se consulta el número de orden de venta en el archivo drive')
# for rec in tabla:
#// dv_uuid = rec['UUID']
#// dv_nm_orden = rec['NmOrdenID']
#//
#//
#//

#Se extrae el ID de la venta en Odoo realizando un match con el numero de orde = yuju marketplace reference
    dv_nm_orden = 8001232222
    so_domain = ['channel_order_reference', '=', dv_nm_orden]
    sale_ids = models.execute_kw(db_name, uid, password, 'sale.order', 'search_read',[[so_domain]],{'fields':['id','name']})

    if sale_ids:
        sale_id = int(sale_ids[0])
        print('Número de venta en Odoo: ', sale_ids[1])

        #Creacion de factura a partir de la venta
        inv_values = [{
            'sale_id': sale_id, #Solo ejemplo
        }]

        create_inv = models.execute_kw(db_name, uid, password, 'account_move', 'create', [inv_values])
        move_id = int(create_inv)
        print('Factura creada con ID: ',create_inv)
        print('')

        invoices_folder = dir_path + '/xml/'
        for rec in os.listdir(invoices_folder):
            xml_name = str(rec)
            if dv_uuid in xml_name:
                if rec.endswith('.xml'):
                    # Read the contents of the XML file
                    with open(os.path.join(invoices_folder, rec), 'rb') as f:
                        xml_data = f.read()

                xml_base64 = base64.b64encode(xml_data).decode('utf-8')

                attachment_data = {
                    'name': xml_name,
                    'datas': xml_base64,
                    'res_model': 'account.move',
                    'res_id': move_id,
                }

                attachment_ids = models.execute_kw(db_name, uid, password, 'ir.attachment', 'create', [attachment_data])
                attachment_id = int(attachment_ids)
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
                print('Valores para la tabla Documentos EDI: ',values)
                print('Registro account.edi.document creado')
                #Invoice status is updated to posted
                time.sleep(1)
                upd_invoice_state = models.execute_kw(db_name, uid, password, 'account.move', 'write', [[move_id],{'state': 'posted'}])
                print('Se publica la factura: ', move_id)
            else:
                print('El nombre del XML no coincide con el UUID del drive: ',dv_uuid)
                pass
    else:
        print('El ID de la orden de MP: ', dv_nm_orden,'no coincide con ninguna venta en Odoo')
        pass
time.sleep(3)

#Incersion de XMLs a facturas

#Location of the xml files
invoices_folder = dir_path + '/xml/'
for rec in os.listdir(invoices_folder):
    xml_name = str(rec)
    if rec.endswith('.xml'):
        # Read the contents of the XML file
        with open(os.path.join(invoices_folder, rec), 'rb') as f:
            xml_data = f.read()

    xml_base64 = base64.b64encode(xml_data).decode('utf-8')

attachment_data = {
    'name': xml_name,
    'datas': xml_base64,
    'res_model': 'account.move',
    'res_id': move_id,
}
#Check order number in sales
sale_field = ['id', '=', sale_id]
sale = models.execute_kw(db_name, uid, password, 'sale.order', 'search_read',
                         [[sale_field]],
                         {'fields':
                              ['id',
                               ]})
# This location will need to be replaced by a for loop
sale_id = sale[0]['id']
#Creation of attachment line
attachment_ids = models.execute_kw(db_name, uid, password, 'ir.attachment', 'create', [attachment_data])

time.sleep(3)
#Search current attachment id
fields = ['name', 'like', '019F34DC-A765-477A-A339-35FB7E673D64@1000000000XX0']
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