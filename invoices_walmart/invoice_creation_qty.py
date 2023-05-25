import time

from flask import Flask, render_template, request, make_response, url_for, session
from os import listdir
from os.path import isfile, join
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
#import MySQLdb
import mysql.connector

#API Configuration
dir_path = os.path.dirname(os.path.realpath(__file__))
server_url  ='https://wonderbrands-v3-8038141.dev.odoo.com'
db_name = 'wonderbrands-v3-8038141'
username = 'admin'
password = 'admin123'

print('-------------------------------------------------------')
print('Conectando API Odoo')
print('-------------------------------------------------------')
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(server_url))
uid = common.authenticate(db_name, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(server_url))
print('Conectando a Mysql')
print('-------------------------------------------------------')
# Connect to MySQL database
mydb = mysql.connector.connect(
  host="wonderbrands1.cuwd36ifbz5t.us-east-1.rds.amazonaws.com",
  user="anibal",
  password="Tuy0TEZOcXAwBgtb",
  database="tech"
)
mycursor = mydb.cursor()
#mycursor.execute("SELECT folio, uuid FROM sr_uuid_walmart WHERE id IN %s", [tuple(sales_order_ids)])
#mycursor.execute("SELECT folio, uuid FROM sr_uuid_walmart limit 3")
print(f"Leyendo query")
print('-------------------------------------------------------')
mycursor.execute("SELECT so_name, uuid FROM sr_so_invoice")
sales_order_records = mycursor.fetchall()
xml_dict = {}
xml_list = []
inv_list = []
value_position = 0
for row in sales_order_records:
    so_name = row[0]
    xml_name = row[1]

    if so_name not in xml_dict:
        xml_dict[so_name] = []

    xml_dict[so_name].append(xml_name)
for so_order, xml_files in xml_dict.items():
    so_domain = ['name', '=', so_order]
    for xml_ids in so_order[1]:
        xml_list.append(xml_ids)
    sale_ids = models.execute_kw(db_name, uid, password,'sale.order', 'search_read', [[so_domain]])
    order_name = sale_ids[0]['name']
    order_state = sale_ids[0]['state']
    print(f"Orden de venta encontrada en el sistema")
    print('-------------------------------------------------------')
    try:
        if order_state == 'done':
            if sale_ids:
                invoice_count = sale_ids[0]['invoice_count']
                if invoice_count < 1:
                    # Create invoice for sales order
                    sale_id = int(sale_ids[0]['id'])
                    #for sale_order in sale_ids:
                    #sale_id = int(sale_order['id'])
                    currency_id = sale_ids[0]['currency_id'][0]
                    narration = sale_ids[0]['note']
                    campaign_id = False
                    medium_id = sale_ids[0]['medium_id']
                    source_id = sale_ids[0]['source_id']
                    user_id = sale_ids[0]['user_id'][0]
                    invoice_user_id = sale_ids[0]['user_id'][0]
                    team_id = sale_ids[0]['team_id'][0]
                    partner_id = sale_ids[0]['partner_id'][0]
                    partner_shipping_id = sale_ids[0]['partner_shipping_id'][0]
                    fiscal_position_id = sale_ids[0]['fiscal_position_id']
                    partner_bank_id = 1
                    journal_id = 1
                    invoice_origin = sale_ids[0]['name']
                    invoice_payment_term_id = sale_ids[0]['payment_term_id']
                    payment_reference = sale_ids[0]['reference']
                    transaction_ids = sale_ids[0]['transaction_ids']
                    company_id = 1
                    sale_order_line_id = sale_ids[0]['order_line']
                    #sale_order_line_change =  sale_ids[0]['order_line'][0]
                    # Call to sale order line to get order line data
                    sol_domain = ['id', 'in', sale_order_line_id]
                    sale_order_line = models.execute_kw(db_name, uid, password, 'sale.order.line', 'search_read', [[sol_domain]])
                    for inv_lines in sale_order_line:
                        qty_delivered = round(inv_lines['qty_delivered'])
                        qty_uom = round(inv_lines['product_uom_qty'])
                        if qty_delivered != 0:
                            for qty in range(qty_delivered):
                                print("DATOS DE FACTURA")
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
                                    'invoice_line_ids': [],
                                    'company_id': company_id,
                                }
                                #line_id = sale_order_line[0]['id']
                                line_id = inv_lines['id']
                                invoice_lines = {'display_type': inv_lines['display_type'],
                                                 'sequence': inv_lines['sequence'],
                                                 'name': inv_lines['name'],
                                                 'product_id': inv_lines['product_id'][0],
                                                 'product_uom_id': inv_lines['product_uom'][0],
                                                 #'quantity': sale_order_line[0]['product_qty'],
                                                 'quantity': 1,
                                                 'discount': inv_lines['discount'],
                                                 'price_unit': inv_lines['price_unit'],
                                                 'tax_ids': [(6, 0, [inv_lines['tax_id'][0]])],
                                                 'analytic_tag_ids': [(6, 0, inv_lines['analytic_tag_ids'])],
                                                 'sale_line_ids': [(4, line_id)],
                                                 }
                                invoice['invoice_line_ids'].append((0, 0, invoice_lines))
                                create_inv = models.execute_kw(db_name, uid, password, 'account.move', 'create', [invoice])
                                print('La factura de la orden: ', invoice_origin, 'fue creada con ID: ', create_inv)
                                #Busca la factura para agregar mensaje en el chatter
                                print(f"Agregando mensaje a la factura")
                                search_inv = models.execute_kw(db_name, uid, password, 'account.move', 'search_read', [[['id', '=', create_inv]]])
                                message = {
                                    'body': 'Esta factura fue creada por el equipo de Tech vía API',
                                    'message_type': 'comment',
                                }
                                write_msg_inv = models.execute_kw(db_name, uid, password, 'account.move', 'message_post', [create_inv], message)
                                # Busca si hay un UUID relacionada con la factura
                                if xml_files:
                                    file_name = xml_files[value_position]
                                    print(f"AGREGANDO ARCHIVO XML A LA FACTURA")
                                    invoices_folder = dir_path + '/xml/'
                                    print(f"El xml {file_name} será agregado a la factura")

                                    xml_file = file_name + '.xml'
                                    xml_file_path = os.path.join(invoices_folder, xml_file)
                                    with open(xml_file_path, 'rb') as f:
                                        xml_data = f.read()
                                    xml_base64 = base64.b64encode(xml_data).decode('utf-8')

                                    attachment_data = {
                                        'name': xml_file,
                                        'datas': xml_base64,
                                        'res_model': 'account.move',
                                        'res_id': create_inv,
                                    }

                                    attachment_ids = models.execute_kw(db_name, uid, password, 'ir.attachment', 'create', [attachment_data])
                                    attachment_id = int(attachment_ids)
                                    values = [{
                                        'move_id': create_inv,
                                        'edi_format_id': 2,
                                        'attachment_id': attachment_id,
                                        'state': 'sent',
                                        'create_uid': 1,
                                        'write_uid': 2,
                                    }]
                                    print('AGREGANDO REGISTRO XML A LA TABLA DOCUMENTOS EDI')
                                    edi_document = models.execute_kw(db_name, uid, password, 'account.edi.document', 'create', values)
                                    print('Valores para la tabla Documentos EDI: ', values)
                                    print('Registro account.edi.document creado')
                                    upd_invoice_state = models.execute_kw(db_name, uid, password, 'account.move', 'write',[[create_inv], {'state': 'posted'}])
                                    print('Se publica la factura: ', create_inv)
                                    value_position += 1
                                    print(f"ESTE ES LA POSICION DEL ARRAY: {value_position}")
                                    print('-------------------------------------------------------')
                                else:
                                    print(f'La orden: {order_name} no tiene un XML en la carpeta')
                                    continue
                        else:
                            for qty in range(qty_uom):
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
                                    'invoice_line_ids': [],
                                    'company_id': company_id,
                                }
                                # line_id = sale_order_line[0]['id']
                                line_id = inv_lines['id']
                                invoice_lines = {'display_type': inv_lines['display_type'],
                                                 'sequence': inv_lines['sequence'],
                                                 'name': inv_lines['name'],
                                                 'product_id': inv_lines['product_id'][0],
                                                 'product_uom_id': inv_lines['product_uom'][0],
                                                 # 'quantity': sale_order_line[0]['product_qty'],
                                                 'quantity': 1,
                                                 'discount': inv_lines['discount'],
                                                 'price_unit': inv_lines['price_unit'],
                                                 'tax_ids': [(6, 0, [inv_lines['tax_id'][0]])],
                                                 'analytic_tag_ids': [(6, 0, inv_lines['analytic_tag_ids'])],
                                                 'sale_line_ids': [(4, line_id)],
                                                 }
                                invoice['invoice_line_ids'].append((0, 0, invoice_lines))
                                create_inv = models.execute_kw(db_name, uid, password, 'account.move', 'create', [invoice])
                                print('La factura de la orden: ', invoice_origin, 'fue creada con ID: ', create_inv)
                                # Search invoice in account_move
                                search_inv = models.execute_kw(db_name, uid, password, 'account.move', 'search_read',
                                                               [[['id', '=', create_inv]]])
                                message = {
                                    'body': 'Esta factura fue creada por el equipo de Tech vía API',
                                    'message_type': 'comment',
                                }
                                write_msg_inv = models.execute_kw(db_name, uid, password, 'account.move', 'message_post',
                                                                  [create_inv], message)
                else:
                    print(f'La orden de venta: {order_name} ya tiene una factura creada')
            else:
                print(f'El ID de la orden: {order_name} no coincide con ninguna venta en Odoo')
                pass
        else:
            print(f"Revise el estatus de la orden {order_name} está en estatus {order_state}")
            print(f"Por lo que esta orden no puede ser facturada")
    except Exception as e:
        print(f"Error al crear la factura de la orden {order_name}: {e}")

mydb.close()
