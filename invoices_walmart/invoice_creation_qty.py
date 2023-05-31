import time

from flask import Flask, render_template, request, make_response, url_for, session
from os import listdir
from os.path import isfile, join
from datetime import date, datetime, timedelta
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
server_url  ='https://wonderbrands-v3-8446418.dev.odoo.com'
db_name = 'wonderbrands-v3-8446418'
username = 'admin'
password = 'admin123'
print('----------------------------------------------------------------')
print('SCRIPT DE CREACIÓN DE FACTURAS POR ITEM')
print('----------------------------------------------------------------')
print('Conectando API Odoo')
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(server_url))
uid = common.authenticate(db_name, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(server_url))
print(common)
print('Conexión con Odoo establecida')
print('----------------------------------------------------------------')
print('Conectando a Mysql')
print('----------------------------------------------------------------')
# Connect to MySQL database
mydb = mysql.connector.connect(
  host="wonderbrands1.cuwd36ifbz5t.us-east-1.rds.amazonaws.com",
  user="anibal",
  password="Tuy0TEZOcXAwBgtb",
  database="tech"
)
mycursor = mydb.cursor()
print(f"Leyendo query")
print('Este proceso tomará algo de tiempo, le recomendamos ir por un café')
print('----------------------------------------------------------------')
mycursor.execute("""select b.order_id, a.uuid
                    from finance.sr_sat_emitidas a
                    left join somos_reyes.odoo_master_txns_c b
                        on a.folio = b.marketplace_order_id
                    where a.serie = 'PGA'
                        AND a.folio in
                        (105636657368, 106353470859, 106360180558, 106360577933, 106365944618, 106369393202, 106375643829, 106390729773, 106395992286, 106413143778, 106429128897, 106433302428, 106437801082, 106444999881, 106448877560, 106452874572, 106457531696, 106457532090, 106457532119, 106457532270, 106457532298, 106462440360, 106484073062, 316234200572, 318234100917, 319232500717, 321232501406, 321233700534, 321234301413, 322232601233, 322233800925, 323232901167, 323233200141, 323233500088, 323233801041, 323234001530, 324232400981, 324232800991, 324234101460, 324234503978, 325234300317, 326234401173, 327232500146, 327234601443, 327234700308, 328232601588, 328233200083, 328234500630, 329232700904, 329234100718, 330232500604, 330232600617, 330233700143, 330234100020, 332233600064, 332233700010, 332233700035, 332233901783, 332234000746, 332234101051, 332234300913, 334234000986, 334234301026, 335233800958, 335233801139, 335233901850, 335234001192, 335234501350, 336233401388, 336234002167, 336234300350, 337232400579, 337232401345, 337232901461, 337232901464, 337232901467, 337232902209, 337234602278, 337234700466, 338232800104, 338233000007, 338234601272, 339232400572, 339232700466, 340232800982, 342233700269, 342234501502, 343232401128, 343234200939, 343234600837, 344232901343, 346232401270, 346232800975, 346234603160, 681236101007, 685235000566, 685236700922, 686235000230, 686236600110, 687234800291, 687235700045, 688235200165, 688235400276, 688236900024, 689236800836, 690236200942, 690236600578, 690237003028, 691236301309, 691236601285, 691237101623, 692236800680, 693235100758, 693236501248, 694235500152, 694236900044, 695235000715, 695236801269, 695236900130, 695236900231, 695236900985, 695237000251, 696236800712, 697235300412, 697236300924, 697236600956, 697236600964, 697236701365, 698236501234, 698236600614, 699235300650, 700237001015, 701235500774, 701236702396, 701236901679, 702235203809, 702236401342, 703235300096, 703236700957, 704236501909, 704236501915, 704236901291, 705236602407, 705237000926, 706236701421, 707236200674, 708235301367, 710236000348, 710236401867, 713234802021)
                    group by a.uuid
                    order by b.order_id""")
sales_order_records = mycursor.fetchall()
xml_dict = {}
xml_list = []
inv_list = []
for row in sales_order_records:
    so_name = row[0]
    xml_name = row[1]

    if so_name not in xml_dict:
        xml_dict[so_name] = []

    xml_dict[so_name].append(xml_name)
for so_order, xml_files in xml_dict.items():
    value_position = 0
    so_domain = ['name', '=', so_order]
    for xml_ids in so_order[1]:
        xml_list.append(xml_ids)
    sale_ids = models.execute_kw(db_name, uid, password,'sale.order', 'search_read', [[so_domain]])
    order_name = sale_ids[0]['name']
    order_state = sale_ids[0]['state']
    print(f"Orden de venta encontrada en el sistema")
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
                                    #invoices_folder = dir_path + '/xml/'
                                    invoices_folder = 'G:/Mi unidad/xml_pga_invoices/'
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
                                    #upd_invoice_state = models.execute_kw(db_name, uid, password, 'account.move', 'write',[[create_inv], {'state': 'posted'}])
                                    upd_invoice_state = models.execute_kw(db_name, uid, password, 'account.move','action_post', [create_inv])
                                    print('Se publica la factura: ', create_inv)
                                    value_position += 1
                                    #print(f"ESTE ES LA POSICION DEL ARRAY: {value_position}")
                                    print('-------------------------------------------------------')
                                else:
                                    print(f'La orden: {order_name} no tiene un XML en la carpeta')
                                    continue
                        else:
                            print(f"La cantidad entregada es igual a {qty_delivered}")
                            print(f"Se tomará en cuenta el campo qty_uom")
                            for qty in range(qty_uom):
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
                                create_inv = models.execute_kw(db_name, uid, password, 'account.move', 'create',
                                                               [invoice])
                                print('La factura de la orden: ', invoice_origin, 'fue creada con ID: ', create_inv)
                                # Busca la factura para agregar mensaje en el chatter
                                print(f"Agregando mensaje a la factura")
                                search_inv = models.execute_kw(db_name, uid, password, 'account.move', 'search_read',
                                                               [[['id', '=', create_inv]]])
                                message = {
                                    'body': 'Esta factura fue creada por el equipo de Tech vía API',
                                    'message_type': 'comment',
                                }
                                write_msg_inv = models.execute_kw(db_name, uid, password, 'account.move',
                                                                  'message_post', [create_inv], message)
                                # Busca si hay un UUID relacionada con la factura
                                if xml_files:
                                    file_name = xml_files[value_position]
                                    print(f"AGREGANDO ARCHIVO XML A LA FACTURA")
                                    # invoices_folder = dir_path + '/xml/'
                                    invoices_folder = 'G:/Mi unidad/xml_pga_invoices/'
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

                                    attachment_ids = models.execute_kw(db_name, uid, password, 'ir.attachment',
                                                                       'create', [attachment_data])
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
                                    edi_document = models.execute_kw(db_name, uid, password, 'account.edi.document',
                                                                     'create', values)
                                    print('Valores para la tabla Documentos EDI: ', values)
                                    print('Registro account.edi.document creado')
                                    # upd_invoice_state = models.execute_kw(db_name, uid, password, 'account.move', 'write',[[create_inv], {'state': 'posted'}])
                                    upd_invoice_state = models.execute_kw(db_name, uid, password, 'account.move', 'action_post', [create_inv])
                                    print('Se publica la factura: ', create_inv)
                                    value_position += 1
                                    # print(f"ESTE ES LA POSICION DEL ARRAY: {value_position}")
                                    print('-------------------------------------------------------')
                                else:
                                    print(f'La orden: {order_name} no tiene un XML en la carpeta')
                                    continue
                else:
                    print(f'La orden de venta: {order_name} ya tiene una factura creada')
                    print('----------------------------------------------------------------')
                    continue
            else:
                print(f'El ID de la orden: {order_name} no coincide con ninguna venta en Odoo')
                continue
        else:
            print(f"Revise el estatus de la orden {order_name} se encuentra en estatus {order_state}")
            print(f"Por lo que esta orden no puede ser facturada")
            continue
    except Exception as e:
        print(f"Error al crear la factura de la orden {order_name}: {e}")

mycursor.close()
mydb.close()
