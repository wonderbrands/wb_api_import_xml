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

#server_url  ='https://wonderbrands.odoo.com'
#db_name = 'wonderbrands-main-4539884'
#username = 'admin'
#password = 'admin123'

server_url  ='https://wonderbrands-v3-8474788.dev.odoo.com'
db_name = 'wonderbrands-v3-8474788'
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
mycursor.execute("""select b.order_id, a.uuid, a.fecha
                    from finance.sr_sat_emitidas a
                    left join somos_reyes.odoo_master_txns_c b
                        on a.folio = b.marketplace_order_id
                    where a.serie = 'PGA'
                        AND a.folio in
                        (106378974781,106378974781,106378974781,685235000566,687234800291,106353470859,106360577933,319232500717,106360180558,321234301413,689235100192,687235700045,686236600110,688235400276,688235400276,688235400276,688235400276,688235400276,690236200942,690236600578,323233200141,323233801041,323233801041,323233801041,323233801041,323233801041,323234001530,696234801204,325234300317,326234401173,692236800680,106390729773,324232800991,693236501248,328234300491,693236501248,328232601588,316234200572,330232600617,330232500604,329232700904,323233500088,106413143778,689236800836,695235000715,329232700904,323233500088,106413143778,695236900985,330233700143,695236900231,106413143778,327232500146,697235300412,697235300412,695237000251,318234100917,106413143778,324234101460,695236900130,691237101623,686234801035,695236801269,695236801269,328233200083,322233800925,106433302428,106365944618,332233700010,332233600064,328234500630,327234601443,328234500630,106433302428,694236900044,321233700534,335234101332,328233200083,691235702356,328234500630,328233200083,330234100020,686235000230,106437801082,335233901850,694235500152,332234101051,335233801139,698236501234,335233801139,332234101051,335233801139,688236900024,321232501406,106429128897,686235000230,322232601233,106369393202,106444999881,335233801139,701236901679,332233901783,699235300650,335233801139,699235300650,324232400981,106448877560,701235500774,334234301026,702235203809,701235500774,685236700922,702236401342,693235100758,334234000986,323232901167,702236401342,691236301309,337232400579,106457532119,106457532119,339232400572,106457532298,337234602278,332234000746,335234501350,106457532090,338233000007,106457531696,106452874572,338232800104,106457532270,332234000746,106457532270,106457531696,106457532090,106452874572,336234300350,106457532119,105636657368,106457532270,106457531696,106457532298,105636657368,332234000746,106452874572,106457532298,337234602278,106457532090,106457531696,106457532119,105636657368,335234001192,106457531696,106457532270,106457532090,704236501915,106457532119,105636657368,704236501915,336233401388,704236501915,106457532298,106457532270,335233800958,106457532298,106457532090,703236700957,106462440360,313234500077,697236300924,703235300096,701236702396,337232901461,337232902209,337232901461,337232902209,704236901291,705236602407,337232902209,704236901291,697236300924,337232901461,333234401479,336234002167,703235300096,337232901461,337232901461,337232902209,703235300096,703235300096,337232902209,324234503978,690237003028,703235300096,340232800982,696236800712,697236701365,696236800712,697236600956,329234100718,704236501909,339232700466,698236600614,706236701421,339232700466,697236600964,327234700308,339232700466,339232700466,106395992286,705237000926,342234501502,338234601272,707236200674,338234601272,338234601272,700237001015,332234300913,708235301367,337232901464,337232901467,337232901464,337232401345,337232401345,343234600837,337232901464,337232901467,337232901464,337232401345,337232901464,337232401345,337232401345,337232901467,337232901467,337232901467,106484073062,343232401128,343234200939,106484073062,316232600332,316232600332,710236000348,106375643829,691236601285,710236401867,346232401270,688235200165,349233701533,346232401270,342233700269,344232901343,346234603160,346232401270,708236801161,337234700466,346232401270,346232800975,713234802021,698235207052,714237100719,342232501493,349233702748,347233101230,349233801209,715236700328,694236701303,349234201564,694236701303,694236701303,714234902104,106508067399,689236400607,698235100069,349234201589,105892453372,714234902438,698235207052,349234201564,347234501751,347234102307,710237000715,332233700035,714235304404,715234900994,681236101007)
                    group by a.uuid
                    order by b.order_id""")
sales_order_records = mycursor.fetchall()
xml_dict = {}
xml_list = []
inv_list = []
sales_error_state = []
sales_no_exist = []
sales_w_inv = []
sales_no_xml = []
sales_mod = []
inv_names = []
date_year = '20'
for row in sales_order_records:
    so_name = row[0]
    xml_name = row[1]
    xml_date = date_year + row[2].strftime("%y-%m-%d %H:%M:%S")

    if so_name not in xml_dict:
        xml_dict[so_name] = []

    xml_dict[so_name].append(xml_name)
    xml_dict[so_name].append(xml_date)
for so_order, xml_files in xml_dict.items():
    value_position = 0
    value_position_date = 1
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
                                    file_date = xml_files[value_position_date]
                                    file_name_mayus = file_name.upper()
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
                                    print(f"Se agrega el folio fiscal: {file_name_mayus}")
                                    #upd_folio_fiscal = models.execute_kw(db_name, uid, password, 'account.move','write', [[create_inv], {'|10n_mx_edi_cfdi_uuid': file_name_mayus}])
                                    upd_folio_fiscal = models.execute_kw(db_name, uid, password, 'account.move','write', [[create_inv], {'l10n_mx_edi_cfdi_uuid': file_name_mayus}])
                                    #Parche momentaneo
                                    upd_folio_fiscal_narr = models.execute_kw(db_name, uid, password, 'account.move','write', [[create_inv], {'narration': file_name_mayus}])
                                    #Actualiza Fecha de factura
                                    print(f"Se Modifica la fecha de factura: {file_date}")
                                    upd_inv_date = models.execute_kw(db_name, uid, password, 'account.move','write', [[create_inv], {'invoice_date': file_date}])
                                    search_inv_name = models.execute_kw(db_name, uid, password, 'account.move','search_read', [[['id', '=', create_inv]]])
                                    inv_name = search_inv_name[0]['name']
                                    # Actualiza Fecha de vencimiento
                                    #print(f"Se Modifica la fecha de factura: {file_date}")
                                    #upd_inv_date = models.execute_kw(db_name, uid, password, 'account.move', 'write',[[create_inv], {'invoice_payment_term_id': file_date}])
                                    #posiciones de los array
                                    value_position += 2
                                    value_position_date += 2
                                    sales_mod.append(order_name)
                                    inv_names.append(inv_name)
                                    #print(f"ESTE ES LA POSICION DEL ARRAY: {value_position}")
                                    print('-------------------------------------------------------')
                                else:
                                    print(f'La orden: {order_name} no tiene un XML en la carpeta')
                                    sales_no_xml.append(order_name)
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
                                    file_name_mayus = file_name.upper()
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
                                    edi_document = models.execute_kw(db_name, uid, password, 'account.edi.document',
                                                                     'create', values)
                                    print('Valores para la tabla Documentos EDI: ', values)
                                    print('Registro account.edi.document creado')
                                    # upd_invoice_state = models.execute_kw(db_name, uid, password, 'account.move', 'write',[[create_inv], {'state': 'posted'}])
                                    upd_invoice_state = models.execute_kw(db_name, uid, password, 'account.move', 'action_post', [create_inv])
                                    print(f"Se publica la factura: {create_inv}")
                                    value_position += 1
                                    print(f"Se agrega el folio fiscal: {file_name_mayus}")
                                    upd_folio_fiscal = models.execute_kw(db_name, uid, password, 'account.move','write', [[create_inv], {'l10n_mx_edi_cfdi_uuid': file_name_mayus}])
                                    # print(f"ESTE ES LA POSICION DEL ARRAY: {value_position}")
                                    print('-------------------------------------------------------')
                                else:
                                    print(f'La orden: {order_name} no tiene un XML en la carpeta')
                                    continue
                else:
                    print(f'La orden de venta: {order_name} ya tiene una factura creada')
                    print('----------------------------------------------------------------')
                    sales_w_inv.append(order_name)
                    continue
            else:
                print(f'El ID de la orden: {order_name} no coincide con ninguna venta en Odoo')
                sales_no_exist.append(order_name)
                continue
        else:
            print(f"Revise el estatus de la orden {order_name} se encuentra en estatus {order_state}")
            print(f"Por lo que esta orden no puede ser facturada")
            sales_error_state.append(order_name)
            continue
    except Exception as e:
        print(f"Error al crear la factura de la orden {order_name}: {e}")

print(f"Ordenes que tienen no están en done {sales_error_state}")
print(f"Ordenes que no existen en Odoo {sales_no_exist}")
print(f"Ordenes que ya tenían una factura {sales_w_inv}")
print(f"Ordenes sin XML {sales_no_xml}")
print(f"Ordenes a las que se les creo factura: {sales_mod}")
print(f"Nombre de las facturas creadas: {inv_names}")

mycursor.close()
mydb.close()
