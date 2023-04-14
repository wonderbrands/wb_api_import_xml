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
import openpyxl
import xlrd
import pandas as pd
import MySQLdb


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
#Conexion con API Google Drive
print('Conexion a Mysql/Somos_Reyes/crawl_ml_colecta_prepare')
print('')

DB = MySQLdb.connect('wonderbrands1.cuwd36ifbz5t.us-east-1.rds.amazonaws.com', 'will', 'RClTFPNeongrVSko',
                     'tech', local_infile=True)
cursor = DB.cursor()

DB.set_character_set('utf8mb4')
cursor.execute('SET NAMES utf8mb4;')
cursor.execute('SET CHARACTER SET utf8mb4;')
cursor.execute('SET character_set_connection=utf8mb4;')

#print('Conexion con la API de Google')
# Define the path to the Excel file containing the orders and Read Excel file
excel_file_path = dir_path + '/files/SO_data.xlsx'
invoices_folder = dir_path + '/xml/'
sale_file = pd.read_excel(excel_file_path, usecols=['id','XML name'])
sale_id_file = sale_file['id'].tolist()
xml_id_file = sale_file['XML name'].tolist()
so_domain = ['id', 'in', sale_id_file]
# sale_ids = models.execute_kw(db_name, uid, password,'sale.order', 'search_read', [[so_domain]], {'fields': ['id', 'name','partner_id', 'order_line']})
sale_ids = models.execute_kw(db_name, uid, password,'sale.order', 'search_read', [[so_domain]])

if sale_ids:
    sale_id = int(sale_ids[0]['id'])
    # Create invoice for sales order
    for sale_order in sale_ids:
        sale_id = int(sale_order['id'])
        currency_id = int(sale_order['currency_id'][0])
        narration = sale_order['note']
        campaign_id = False
        medium_id = sale_order['medium_id']
        source_id = sale_order['source_id']
        user_id = int(sale_order['user_id'][0])
        invoice_user_id = int(sale_order['user_id'][0])
        team_id = int(sale_order['team_id'][0])
        partner_id = int(sale_order['partner_id'][0])
        partner_shipping_id = int(sale_order['partner_shipping_id'][0])
        fiscal_position_id = sale_order['fiscal_position_id']
        partner_bank_id = 1
        journal_id = 1
        invoice_origin = sale_order['name']
        invoice_payment_term_id = sale_order['payment_term_id']
        payment_reference = sale_order['reference']
        transaction_ids = sale_order['transaction_ids']
        company_id = 1
        sale_order_line_id = sale_order['order_line']
        sale_order_line_change =  int(sale_order['order_line'][0])
        # Call to sale order line to get order line data
        sol_domain = ['id', '=', sale_order_line_id]
        sale_order_line = models.execute_kw(db_name, uid, password, 'sale.order.line', 'search_read', [[sol_domain]])
        # Define the invoice values
        invoice = {
            'ref': '',
            'move_type': 'out_invoice',
            'currency_id': currency_id,
            'narration': narration,
            'campaign_id': campaign_id,
            'medium_id': medium_id,
            'source_id': source_id,
            'user_id': user_id,
            'invoice_user_id': invoice_user_id,
            'team_id': team_id,
            'partner_id': partner_id,
            'partner_shipping_id': partner_shipping_id,
            'fiscal_position_id': fiscal_position_id,
            'partner_bank_id': partner_bank_id,
            'journal_id': journal_id,  # company comes from the journal
            'invoice_origin': invoice_origin,
            'invoice_payment_term_id': invoice_payment_term_id,
            'payment_reference': payment_reference,
            'transaction_ids': [(6, 0, transaction_ids)],
            'invoice_line_ids': [(0,0,{
                'display_type': sale_order_line[0]['display_type'],
                'sequence': sale_order_line[0]['sequence'],
                'name': sale_order_line[0]['name'],
                'product_id': sale_order_line[0]['product_id'][0],
                'product_uom_id': sale_order_line[0]['product_uom'][0],
                'quantity': sale_order_line[0]['product_qty'],
                'discount': sale_order_line[0]['discount'],
                'price_unit': sale_order_line[0]['price_unit'],
                'tax_ids': [(6, 0, [sale_order_line[0]['tax_id'][0]])],
                'analytic_tag_ids': [(6, 0, sale_order_line[0]['analytic_tag_ids'])],
                'sale_line_ids': [(4, sale_order_line_change)],
                })],
            'company_id': company_id,
        }
        create_inv = models.execute_kw(db_name, uid, password, 'account.move', 'create', [invoice])
        print('La factura de la orden: ', sale_id, 'fue creada con ID: ', create_inv)
        move_id = create_inv
        print('Factura creada con ID: ',move_id)

        dv_uuid = xml_id_file
        for rec in os.listdir(invoices_folder):
            xml_name = str(rec)
            if xml_name in dv_uuid:
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
                    break
                else:
                    pass
else:
    print('El ID de la orden de MP: ', dv_nm_orden,'no coincide con ninguna venta en Odoo')
    pass

DB.close()
