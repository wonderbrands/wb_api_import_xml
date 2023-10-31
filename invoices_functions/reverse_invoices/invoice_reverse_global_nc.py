from flask import Flask, render_template, request, make_response, url_for, session
from email.message import EmailMessage
from email.utils import make_msgid
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from pprint import pprint
from email import encoders
from tqdm import tqdm
import time
import json
import jsonrpc
import jsonrpclib
import random
import urllib.request
import getpass
import http
import requests
import logging
import zipfile
import socket
import os
import locale
import xmlrpc.client
import base64
import openpyxl
import xlrd
import pandas as pd
import MySQLdb
import mysql.connector
import smtplib
import ssl
import email
import datetime

print('================================================================')
print('BIENVENIDO AL PROCESO DE NOTAS DE CRÉDITO PARA MARKETPLACES')
print('================================================================')
print('SCRIPT DE CREACIÓN DE NOTAS DE CRÉDITO PARA FACTURAS GLOBALES')
print('================================================================')
today_date = datetime.datetime.now()
dir_path = os.path.dirname(os.path.realpath(__file__))
print('Fecha:' + today_date.strftime("%Y-%m-%d %H:%M:%S"))
#Archivo de configuración - Use config_dev.json si está haciendo pruebas
#Archivo de configuración - Use config.json cuando los cambios vayan a producción
config_file_name = r'C:\Dev\wb_odoo_external_api\config_dev.json'

def get_odoo_access():
    with open(config_file_name, 'r') as config_file:
        config = json.load(config_file)

    return config['odoo']
def get_psql_access():
    with open(config_file_name, 'r') as config_file:
        config = json.load(config_file)

    return config['psql']
def get_email_access():
    with open(config_file_name, 'r') as config_file:
        config = json.load(config_file)

    return config['email']
def reverse_invoice_global():
    # Obtener credenciales
    odoo_keys = get_odoo_access()
    psql_keys = get_psql_access()
    email_keys = get_email_access()
    # odoo
    server_url = odoo_keys['odoourl']
    db_name = odoo_keys['odoodb']
    username = odoo_keys['odoouser']
    password = odoo_keys['odoopassword']
    # correo
    smtp_server = email_keys['smtp_server']
    smtp_port = email_keys['smtp_port']
    smtp_username = email_keys['smtp_username']
    smtp_password = email_keys['smtp_password']

    print('----------------------------------------------------------------')
    print('Conectando API Odoo')
    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(server_url))
    uid = common.authenticate(db_name, username, password, {})
    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(server_url))
    print('Conexión con Odoo establecida')
    print('----------------------------------------------------------------')
    print('Conectando a Mysql')
    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host=psql_keys['dbhost'],
        user=psql_keys['dbuser'],
        password=psql_keys['dbpassword'],
        database=psql_keys['database']
    )
    mycursor = mydb.cursor()
    print('----------------------------------------------------------------')
    print('Vaya por un tecito o un café porque este proceso tomará algo de tiempo')

    #mycursor.execute("""SELECT c.name, b.id 'account_move_id', b.name/*d.order_id, a.total, a.subtotal, d.refunded_amt,b.invoice_partner_display_name*/
    #                    FROM finance.sr_sat_emitidas a
    #                    LEFT JOIN somos_reyes.odoo_new_account_move_aux b
    #                    ON a.uuid = b.l10n_mx_edi_cfdi_uuid
    #                    LEFT JOIN somos_reyes.odoo_new_sale_order c
    #                    ON SUBSTRING_INDEX(SUBSTRING_INDEX(invoice_ids, ']', 1), '[', -1) = b.id
    #                    LEFT JOIN (SELECT order_id, status_detail, pay_status, SUM(paid_amt) 'paid_amt', SUM(refunded_amt) 'refunded_amt'
    #                                FROM somos_reyes.ml_order_payments
    #                                WHERE refunded_amt > 0
    #                                GROUP BY 1,2,3) d
    #                    ON c.channel_order_id = d.order_id
    #                    LEFT JOIN (SELECT distinct invoice_origin
    #                                FROM somos_reyes.odoo_new_account_move_aux
    #                                WHERE name like '%RINV%') e
    #                    ON c.name = e.invoice_origin
    #                    WHERE d.order_id is not null
    #                        AND e.invoice_origin is null
    #                        #AND refunded_amt - a.total > 1 OR refunded_amt - a.total < -1
    #                        AND invoice_partner_display_name = 'PÚBLICO EN GENERAL'
    #                        limit 10""")
    #invoice_records = mycursor.fetchall()
    so_no_exist = []
    so_w_refund = []
    inv_names = []
    so_names = []
    nc_created = []
    so_no_exist_in_invoice = []
    print('----------------------------------------------------------------')
    print('Creando notas de crédito')
    print('Este proceso tomará unos minutos')
    invoice_records = [('SO2479520', '821764', 'INV/8202/40340'), ('SO2474777', '821764', 'INV/8202/40340')]
    try:
        progress_bar = tqdm(total=len(invoice_records), desc="Procesando")
        for each in invoice_records:
            inv_origin_name = each[0]
            inv_id = each[1]
            inv_name = each[2]
            inv_move_types = [] # Lista en la que se almacenan los tipos de factura para la orden en curso
            #Busca la factura que contenga el nombre de la SO
            invoice = models.execute_kw(db_name, uid, password, 'account.move', 'search_read', [[['id', '=', inv_id]]])
            if invoice:
                for inv in invoice:
                    if inv_origin_name in inv['invoice_origin']:
                        # Se verifica si ya existe una nota de crédito para esta orden de venta
                        existing_credit_note = models.execute_kw(db_name, uid, password, 'account.move', 'search', [[['invoice_origin', '=', inv_origin_name], ['move_type', '=', 'out_refund']]])
                        if not existing_credit_note:
                            # Obtiene los detalles de la orden de venta actual
                            sale_order = models.execute_kw(db_name, uid, password, 'sale.order', 'search_read',[[['name', '=', inv_origin_name]]], {'fields': ['order_line']})
                            if sale_order:
                                order_lines = sale_order[0]['order_line']
                                inv_uuid = inv['l10n_mx_edi_cfdi_uuid']  # Folio fiscal de la factura
                                inv_journal_id = inv['journal_id'][0]
                                #invoice_lines = models.execute_kw(db_name, uid, password, 'account.move.line','search_read',[[['move_id', '=', inv['id']]]], {'fields': ['sale_line_ids']})
                                # Obtener las líneas de factura relacionadas con la orden de venta actual
                                #order_invoice_lines = [line['sale_line_ids'][0] for line in invoice_lines if line['sale_line_ids']]
                                create_wizard = {
                                        'refund_method': 'refund',
                                        'reason': 'Por efectos de devolución o retorno de una orden',
                                        'journal_id': inv_journal_id,
                                        'line_ids': [],
                                    }
                                for ids in order_lines:
                                    create_wizard['line_ids'].append((6, 0, ids))
                                credit_note_wizard = models.execute_kw(db_name, uid, password,
                                                                       'account.move.reversal',
                                                                       'create', [create_wizard],
                                                                       {'context': {
                                                                           'active_ids': [inv_id],
                                                                           'active_id': inv_id,
                                                                           'active_model': 'account.move',
                                                                       }}
                                                                       )
                                # Se crea la nota de crédito con la info anterior y se usa la función reverse_moves del botón revertir en el wizard
                                nc_inv_create = models.execute_kw(db_name, uid, password, 'account.move.reversal','reverse_moves', [credit_note_wizard])
                                nc_id = nc_inv_create['res_id']  # Obtiene el id de la nota de crédito
                                # Agrega un mensaje al chatter de la nota de crédito
                                message = {
                                    'body': f"Esta nota de crédito fue creada a partir de la factura: {inv_name}, de la órden {inv_origin}, con folio fiscal {inv_uuid}, a solicitud del equipo de Contabilidad, por el equipo de Tech mediante API.",
                                    'message_type': 'comment',
                                }
                                write_msg_tech = models.execute_kw(db_name, uid, password, 'account.move','message_post', [nc_id], message)
                                progress_bar.update(1)
                            else:
                                print(f"No se encontró la orden de venta {inv_origin_name}")
                        else:
                            print(f"La órden {inv_origin_name} ya tiene una nota de crédito creada")
                            so_w_refund.append(inv_origin_name)
                            continue
                    else:
                        print(f"La órden {inv_origin_name} no se encontró en la factura global")
                        so_no_exist_in_invoice.append(inv_origin_name)
                        continue
            else:
                print(f"No hay una factura en la SO {inv_origin_name} por la cual se pueda crear una nota de crédito")
                so_no_exist.append(inv_origin_name)
                continue
    except Exception as e:
       print(f"Error: no se pudo crear la nota de crédito: {e}")

    # Define el cuerpo del correo
    print('Creando correo y excel')
    msg = MIMEMultipart()
    body = '''\
    <html>
      <head></head>
      <body>
        <p>Buenas tardes</p>
        <p>Hola a todos, espero que estén muy bien. Les comento que acabamos de correr el script de notas de crédito.</p>
        <p>Adjunto encontrarán el archivo generado por el script en el cual se encuentran las órdenes a las cuales se les creó una nota de crédito, órdenes que no se les pudo crear una NC y nombre de las notas de crédito creadas.</p>
        </br>
        <p>Sin más por el momento quedo al pendiente para resolver cualquier duda o comentario.</p>
        </br>
        <p>Muchas gracias</p>
        </br>
        <p>Un abrazo</p>
      </body>
    </html>
    '''

    # Crear el archivo Excel y agregar los nombres de los arrays y los resultados
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet['A1'] = 'so_names'
    sheet['B1'] = 'nc_created'
    sheet['C1'] = 'inv_names'
    sheet['D1'] = 'so_w_refund'
    sheet['E1'] = 'so_no_exist'

    # Agregar los resultados de los arrays
    for i in range(len(so_names)):
        sheet['A{}'.format(i + 2)] = so_names[i]
    for i in range(len(nc_created)):
        sheet['B{}'.format(i + 2)] = nc_created[i]
    for i in range(len(inv_names)):
        sheet['C{}'.format(i + 2)] = inv_names[i]
    for i in range(len(so_w_refund)):
        sheet['D{}'.format(i + 2)] = so_w_refund[i]
    for i in range(len(so_no_exist)):
        sheet['E{}'.format(i + 2)] = so_no_exist[i]

    # Guardar el archivo Excel en disco
    excel_file = 'notas_credito_' + today_date.strftime("%Y%m%d") + '.xlsx'
    workbook.save(excel_file)

    # Leer el contenido del archivo Excel
    with open(excel_file, 'rb') as file:
        file_data = file.read()
    file_data_encoded = base64.b64encode(file_data).decode('utf-8')

    # Define remitente y destinatario
    msg = MIMEMultipart()
    msg['From'] = 'Tech anibal@wonderbrands.co'
    msg['To'] = ', '.join(
        ['anibal@wonderbrands.co', 'rosalba@wonderbrands.co', 'natalia@wonderbrands.co', 'greta@somos-reyes.com',
         'contabilidad@somos-reyes.com', 'alex@wonderbrands.co', 'will@wonderbrands.co'])
    msg['Subject'] = 'Creación de notas de crédito mediante script automático'
    # Adjuntar el cuerpo del correo
    msg.attach(MIMEText(body, 'html'))
    # Adjuntar el archivo Excel al mensaje
    attachment = MIMEBase('application', 'octet-stream')
    attachment.set_payload(file_data)
    encoders.encode_base64(attachment)
    attachment.add_header('Content-Disposition', 'attachment', filename=excel_file)
    msg.attach(attachment)
    print("Enviando correo")
    try:
        smtpObj = smtplib.SMTP(smtp_server, smtp_port)
        smtpObj.starttls()
        smtpObj.login(smtp_username, smtp_password)
        smtpObj.sendmail(smtp_username, msg['To'], msg.as_string())
    except Exception as e:
        print(f"Error: no se pudo enviar el correo: {e}")

    print('----------------------------------------------------------------')
    print('Proceso completado')
    print('Este arroz ya se coció :)')
    print('----------------------------------------------------------------')

    # Cierre de conexiones
    progress_bar.close()
    smtpObj.quit()
    mycursor.close()
    mydb.close()

if __name__ == "__main__":
    reverse_invoice_global()