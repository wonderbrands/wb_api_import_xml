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
print('SCRIPT DE CREACIÓN DE NOTAS DE CRÉDITO INDIVIDUALES')
print('================================================================')
today_date = datetime.datetime.now()
dir_path = os.path.dirname(os.path.realpath(__file__))
print('Fecha:' + today_date.strftime("%Y-%m-%d %H:%M:%S"))
#Archivo de configuración - Use config_dev.json si está haciendo pruebas
#Archivo de configuración - Use config.json cuando los cambios vayan a producción
config_file_name = r'C:\Dev\wb_odoo_external_api\config\config_dev.json'

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
def reverse_invoice_meli():
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
    print('NOTAS DE CRÉDITO MELI')
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

    mycursor.execute("""SELECT c.name,
                               b.id 'account_move_id',
                               ifnull(d.payment_date_last_modified, dd.payment_date_last_modified) 'payment_date_last_modified'/*,
                               ifnull(d.order_id, dd.pack_id) 'order_id_or_pack_id',
                               a.total 'total_factura',
                               a.subtotal 'subtotal_factura',
                               ifnull(d.refunded_amt, dd.refunded_amt) 'ml_refunded_amount',
                               b.invoice_partner_display_name 'cliente',
                               b.name*/
                        FROM finance.sr_sat_emitidas a
                        LEFT JOIN somos_reyes.odoo_new_account_move_aux b
                        ON a.uuid = b.l10n_mx_edi_cfdi_uuid
                        LEFT JOIN odoo_new_sale_order c
                        ON b.invoice_origin = c.name
                        LEFT JOIN (SELECT a.order_id, max(payment_date_last_modified) 'payment_date_last_modified', SUM(paid_amt) 'paid_amt', SUM(refunded_amt) 'refunded_amt'
                                   FROM ml_order_payments a
                                   LEFT JOIN ml_order_update b
                                   ON a.order_id = b.order_id
                                   WHERE refunded_amt > 0 AND b.pack_id = 'None' AND date(payment_date_last_modified) >= '2023-01-01' AND date(payment_date_last_modified) <= '2023-09-30'
                                   GROUP BY 1) d
                        ON c.channel_order_id = d.order_id
                        LEFT JOIN (SELECT a.pack_id, max(payment_date_last_modified) 'payment_date_last_modified', SUM(b.paid_amt) 'paid_amt', SUM(b.refunded_amt) 'refunded_amt'
                        FROM ml_order_update a
                        LEFT JOIN ml_order_payments b
                        ON a.order_id = b.order_id
                        WHERE b.refunded_amt > 0 AND a.pack_id <> 'None' AND date(payment_date_last_modified) >= '2023-01-01' AND date(payment_date_last_modified) <= '2023-09-30'
                        GROUP BY 1) dd
                        ON c.yuju_pack_id = dd.pack_id
                        LEFT JOIN (SELECT distinct invoice_origin FROM odoo_new_account_move_aux WHERE name like '%RINV%') e
                        ON c.name = e.invoice_origin
                        WHERE (d.order_id is not null or dd.pack_id is not null)
                        AND e.invoice_origin is null
                        AND ifnull(d.refunded_amt, dd.refunded_amt) - a.total < 1 AND ifnull(d.refunded_amt, dd.refunded_amt) - a.total > -1;""")
    invoice_records = mycursor.fetchall()
    # Lista de SO a las que se les creó una credit_notes
    so_modified = []
    # Lista de las facturas enlazadas a la SO y no existen
    inv_no_exist = []
    # Lista de SO que ya contaban con credit_notes antes del script
    so_with_refund = []
    # Lista de nombres de las notas de crédito creadas
    nc_created = []
    # Lista de facturas origen
    so_origin_invoice = []
    # Lista de referencias MKP para cada SO
    so_mkp_reference = []
    print('----------------------------------------------------------------')
    print('Creando notas de crédito')
    print('Este proceso tomará unos minutos')
    # Creación de notas de crédito
    try:
        progress_bar = tqdm(total=len(invoice_records), desc="Procesando")
        for each in invoice_records:
            inv_origin_name = each[0]
            inv_id = each[1]
            nc_date = each[2].strftime("%Y-%m-%d %H:%M:%S")
            inv_move_types = [] # Lista en la que se almacenan los tipos de factura para la orden en curso
            #Busca la factura que contenga el nombre de la SO
            invoice = models.execute_kw(db_name, uid, password, 'account.move', 'search_read', [[['invoice_origin', '=', inv_origin_name]]])
            if invoice:
                for type in invoice:
                    exist_nc_type = type['move_type']
                    inv_move_types.append(exist_nc_type)

                # Se verifica si ya existe una nota de crédito para esta orden de venta
                existing_credit_note = models.execute_kw(db_name, uid, password, 'account.move', 'search', [[['invoice_origin', '=', inv_origin_name], ['move_type', '=', 'out_refund']]])
                if not existing_credit_note:
                #if 'out_refund' not in inv_move_types:
                    for inv in invoice:
                        inv_id = inv['id'] # ID de la factura
                        inv_name = inv['name'] # Nombre de la factura
                        inv_origin = inv['invoice_origin'] # Nombre de la SO ligada a la factura
                        #inv_narration = inv['narration']
                        #inv_uuid = inv_narration[3:-4]
                        inv_uuid = inv['l10n_mx_edi_cfdi_uuid'] # Folio fiscal de la factura
                        inv_journal_id = inv['journal_id'][0] #Diario de la factura
                        #Se hace una llamada al wizard de creación de notas de crédito
                        credit_note_wizard = models.execute_kw(db_name, uid, password, 'account.move.reversal', 'create',
                                                               [{
                            'refund_method': 'refund',
                            'reason': 'Por efectos de devolución o retorno de una orden',
                            'journal_id': inv_journal_id, }],
                                       {'context': {
                                           'active_ids': [inv_id],
                                           'active_id': inv_id,
                                           'active_model': 'account.move',
                                       }}
                                    )
                        #Se crea la nota de crédito con la info anterior y se usa la función reverse_moves del botón revertir en el wizard
                        nc_inv_create = models.execute_kw(db_name, uid, password, 'account.move.reversal', 'reverse_moves',[credit_note_wizard])
                        nc_id = nc_inv_create['res_id'] # Obtiene el id de la nota de crédito
                        # Agrega un mensaje al chatter de la nota de crédito
                        message = {
                            'body': f"Esta nota de crédito fue creada a partir de la factura: {inv_name}, de la órden {inv_origin}, con folio fiscal {inv_uuid}, a solicitud del equipo de Contabilidad, por el equipo de Tech mediante API.",
                            'message_type': 'comment',
                        }
                        write_msg_tech = models.execute_kw(db_name, uid, password, 'account.move', 'message_post',[nc_id], message)
                        #Confirma la nota de crédito
                        upd_nc_state = models.execute_kw(db_name, uid, password, 'account.move', 'action_post',[nc_id])
                        # Timbramos la nota de crédito
                        # upd_nc_stamp = models.execute_kw(db_name, uid, password, 'account.move', 'button_process_edi_web_services',[nc_id])
                        #buscamos el nombre de la nota creada
                        search_nc_name = models.execute_kw(db_name, uid, password, 'account.move', 'search_read',[[['id', '=', nc_id]]])
                        nc_name = search_nc_name[0]['name']
                        sale_order = models.execute_kw(db_name, uid, password, 'sale.order', 'search_read',[[['name', '=', inv_origin_name]]])[0]
                        sale_ref = sale_order['channel_order_reference']
                        #Agregamos a las listas
                        nc_created.append(nc_name)
                        so_modified.append(inv_origin)
                        so_origin_invoice.append(inv_name)
                        so_mkp_reference.append(sale_ref)
                        progress_bar.update(1)
                else:
                    print(f"La órden {inv_origin_name} ya tiene una nota de crédito creada")
                    so_with_refund.append(inv_origin_name)
                    progress_bar.update(1)
                    continue
            else:
                print(f"No hay una factura en la SO {inv_origin_name} por la cual se pueda crear una nota de crédito")
                inv_no_exist.append(inv_origin_name)
                progress_bar.update(1)
                continue
    except Exception as e:
        print(f"Error: no se pudo crear la nota de crédito: {e}")
    # Excel
    try:
        # Crear el archivo Excel y agregar los nombres de los arrays y los resultados
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet['A1'] = 'so_modified'
        sheet['B1'] = 'so_mkp_reference'
        sheet['C1'] = 'nc_created'
        sheet['D1'] = 'so_origin_invoice'
        sheet['E1'] = 'inv_no_exist'
        sheet['F1'] = 'so_with_refund'

        # Agregar los resultados de los arrays
        for i in range(len(so_modified)):
            sheet['A{}'.format(i + 2)] = so_modified[i]
        for i in range(len(so_mkp_reference)):
            sheet['B{}'.format(i + 2)] = so_mkp_reference[i]
        for i in range(len(nc_created)):
            sheet['C{}'.format(i + 2)] = nc_created[i]
        for i in range(len(so_origin_invoice)):
            sheet['D{}'.format(i + 2)] = so_origin_invoice[i]
        for i in range(len(inv_no_exist)):
            sheet['E{}'.format(i + 2)] = inv_no_exist[i]
        for i in range(len(so_with_refund)):
            sheet['F{}'.format(i + 2)] = so_with_refund[i]

        # Guardar el archivo Excel en disco
        excel_file = 'notas_credito_individuales_meli_' + today_date.strftime("%Y%m%d") + '.xlsx'
        workbook.save(excel_file)

        # Leer el contenido del archivo Excel
        with open(excel_file, 'rb') as file:
            file_data = file.read()
        file_data_encoded = base64.b64encode(file_data).decode('utf-8')
    except Exception as a:
        print(f"Error: no se pudo crear el archivo de excel: {a}")
    # Correo
    try:
        msg = MIMEMultipart()
        body = '''\
                <html>
                  <head></head>
                  <body>
                    <p>Buenas</p>
                    <p>Hola a todos, espero que estén muy bien. Les comento que acabamos de correr el script de notas de crédito.</p>
                    <p>Adjunto encontrarán el archivo generado por el script en el cual se encuentran las órdenes a las cuales 
                    se les creó una nota de crédito, órdenes que no se les pudo crear una credit_notes, nombre de las notas de crédito 
                    creadas, órdenes que ya contaban con una nota de crédito antes de correr el script y órdenes que tuvieron 
                    algún error, por ejemplo que no existieran dentro de la factura global o no tuvieran una factura creada por la cual se pueda emitir una nota de crédito.</p>
                    </br>
                    <p>Sin más por el momento quedo al pendiente para resolver cualquier duda o comentario.</p>
                    </br>
                    <p>Muchas gracias</p>
                    </br>
                    <p>Un abrazo</p>
                  </body>
                </html>
                '''
        # Define remitente y destinatario
        msg = MIMEMultipart()
        msg['From'] = 'Tech anibal@wonderbrands.co'
        msg['To'] = ', '.join(
            ['anibal@wonderbrands.co', 'rosalba@wonderbrands.co', 'natalia@wonderbrands.co', 'greta@somos-reyes.com',
             'contabilidad@somos-reyes.com', 'alex@wonderbrands.co', 'will@wonderbrands.co'])
        msg['Subject'] = 'Script Automático Meli - Creación de notas de crédito para facturas individuales'
        # Adjuntar el cuerpo del correo
        msg.attach(MIMEText(body, 'html'))
        # Adjuntar el archivo Excel al mensaje
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(file_data)
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', 'attachment', filename=excel_file)
        msg.attach(attachment)
        print("Enviando correo")
        smtpObj = smtplib.SMTP(smtp_server, smtp_port)
        smtpObj.starttls()
        smtpObj.login(smtp_username, smtp_password)
        smtpObj.sendmail(smtp_username, msg['To'], msg.as_string())
    except Exception as i:
        print(f"Error: no se pudo enviar el correo: {i}")

    print('----------------------------------------------------------------')
    print('PROCESO NC PARA MERCADO LIBRE COMPLETADO :)')
    print('----------------------------------------------------------------')

    # Cierre de conexiones
    progress_bar.close()
    smtpObj.quit()
    mycursor.close()
    mydb.close()
def reverse_invoice_amazon():
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
    print('NOTAS DE CRÉDITO AMAZON')
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

    mycursor.execute("""SELECT c.name,
                               b.id 'account_move_id',
                               d.refund_date as 'payment_date_last_modified'/*,
                               d.order_id 'order_id',
                               a.total 'total_factura',
                               a.subtotal 'subtotal_factura',
                               d.refunded_amt,
                               b.invoice_partner_display_name 'cliente',
                               b.name*/
                        FROM finance.sr_sat_emitidas a
                        LEFT JOIN somos_reyes.odoo_new_account_move_aux b
                        ON a.uuid = b.l10n_mx_edi_cfdi_uuid
                        LEFT JOIN odoo_new_sale_order c
                        ON b.invoice_origin = c.name
                        LEFT JOIN (SELECT a.order_id, max(STR_TO_DATE(fecha, '%d/%m/%Y')) 'refund_date', SUM(total - tarifas_de_amazon) * (-1) 'refunded_amt'
                                   FROM somos_reyes.amazon_payments_refunds a
                                   WHERE (total - tarifas_de_amazon) * (-1) > 0 AND STR_TO_DATE(fecha, '%d/%m/%Y') >= '2023-01-01' AND STR_TO_DATE(fecha, '%d/%m/%Y') <= '2023-09-30'
                                   GROUP BY 1) d
                        ON c.channel_order_id = d.order_id
                        LEFT JOIN (SELECT distinct invoice_origin FROM odoo_new_account_move_aux WHERE name like '%RINV%') e
                        ON c.name = e.invoice_origin
                        WHERE d.order_id is not null
                        AND e.invoice_origin is null
                        AND d.refunded_amt - a.total < 1 AND d.refunded_amt - a.total > -1;""")
    invoice_records = mycursor.fetchall()
    # Lista de SO a las que se les creó una credit_notes
    so_modified = []
    # Lista de las facturas enlazadas a la SO y no existen
    inv_no_exist = []
    # Lista de SO que ya contaban con credit_notes antes del script
    so_with_refund = []
    # Lista de nombres de las notas de crédito creadas
    nc_created = []
    # Lista de facturas origen
    so_origin_invoice = []
    # Lista de referencias MKP para cada SO
    so_mkp_reference = []
    print('----------------------------------------------------------------')
    print('Creando notas de crédito')
    print('Este proceso tomará unos minutos')
    # Creación de notas de crédito
    try:
        progress_bar = tqdm(total=len(invoice_records), desc="Procesando")
        for each in invoice_records:
            inv_origin_name = each[0]
            inv_id = each[1]
            nc_date = each[2].strftime("%Y-%m-%d %H:%M:%S")
            inv_move_types = [] # Lista en la que se almacenan los tipos de factura para la orden en curso
            #Busca la factura que contenga el nombre de la SO
            invoice = models.execute_kw(db_name, uid, password, 'account.move', 'search_read', [[['invoice_origin', '=', inv_origin_name]]])
            if invoice:
                for type in invoice:
                    exist_nc_type = type['move_type']
                    inv_move_types.append(exist_nc_type)

                # Se verifica si ya existe una nota de crédito para esta orden de venta
                existing_credit_note = models.execute_kw(db_name, uid, password, 'account.move', 'search', [[['invoice_origin', '=', inv_origin_name], ['move_type', '=', 'out_refund']]])
                if not existing_credit_note:
                #if 'out_refund' not in inv_move_types:
                    for inv in invoice:
                        inv_id = inv['id'] # ID de la factura
                        inv_name = inv['name'] # Nombre de la factura
                        inv_origin = inv['invoice_origin'] # Nombre de la SO ligada a la factura
                        #inv_narration = inv['narration']
                        #inv_uuid = inv_narration[3:-4]
                        inv_uuid = inv['l10n_mx_edi_cfdi_uuid'] # Folio fiscal de la factura
                        inv_journal_id = inv['journal_id'][0] #Diario de la factura
                        #Se hace una llamada al wizard de creación de notas de crédito
                        credit_note_wizard = models.execute_kw(db_name, uid, password, 'account.move.reversal', 'create',
                                                               [{
                            'refund_method': 'refund',
                            'reason': 'Por efectos de devolución o retorno de una orden',
                            'journal_id': inv_journal_id, }],
                                       {'context': {
                                           'active_ids': [inv_id],
                                           'active_id': inv_id,
                                           'active_model': 'account.move',
                                       }}
                                    )
                        #Se crea la nota de crédito con la info anterior y se usa la función reverse_moves del botón revertir en el wizard
                        nc_inv_create = models.execute_kw(db_name, uid, password, 'account.move.reversal', 'reverse_moves',[credit_note_wizard])
                        nc_id = nc_inv_create['res_id'] # Obtiene el id de la nota de crédito
                        # Agrega un mensaje al chatter de la nota de crédito
                        message = {
                            'body': f"Esta nota de crédito fue creada a partir de la factura: {inv_name}, de la órden {inv_origin}, con folio fiscal {inv_uuid}, a solicitud del equipo de Contabilidad, por el equipo de Tech mediante API.",
                            'message_type': 'comment',
                        }
                        write_msg_tech = models.execute_kw(db_name, uid, password, 'account.move', 'message_post',[nc_id], message)
                        #Confirma la nota de crédito
                        upd_nc_state = models.execute_kw(db_name, uid, password, 'account.move', 'action_post',[nc_id])
                        # Timbramos la nota de crédito
                        # upd_nc_stamp = models.execute_kw(db_name, uid, password, 'account.move', 'button_process_edi_web_services',[nc_id])
                        #buscamos el nombre de la nota creada
                        search_nc_name = models.execute_kw(db_name, uid, password, 'account.move', 'search_read',[[['id', '=', nc_id]]])
                        nc_name = search_nc_name[0]['name']
                        sale_order = models.execute_kw(db_name, uid, password, 'sale.order', 'search_read',[[['name', '=', inv_origin_name]]])[0]
                        sale_ref = sale_order['channel_order_reference']
                        #Agregamos a las listas
                        nc_created.append(nc_name)
                        so_modified.append(inv_origin)
                        so_origin_invoice.append(inv_name)
                        so_mkp_reference.append(sale_ref)
                        progress_bar.update(1)
                else:
                    print(f"La órden {inv_origin_name} ya tiene una nota de crédito creada")
                    so_with_refund.append(inv_origin_name)
                    progress_bar.update(1)
                    continue
            else:
                print(f"No hay una factura en la SO {inv_origin_name} por la cual se pueda crear una nota de crédito")
                inv_no_exist.append(inv_origin_name)
                progress_bar.update(1)
                continue
    except Exception as e:
        print(f"Error: no se pudo crear la nota de crédito: {e}")
    # Excel
    try:
        # Crear el archivo Excel y agregar los nombres de los arrays y los resultados
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet['A1'] = 'so_modified'
        sheet['B1'] = 'so_mkp_reference'
        sheet['C1'] = 'nc_created'
        sheet['D1'] = 'so_origin_invoice'
        sheet['E1'] = 'inv_no_exist'
        sheet['F1'] = 'so_with_refund'

        # Agregar los resultados de los arrays
        for i in range(len(so_modified)):
            sheet['A{}'.format(i + 2)] = so_modified[i]
        for i in range(len(so_mkp_reference)):
            sheet['B{}'.format(i + 2)] = so_mkp_reference[i]
        for i in range(len(nc_created)):
            sheet['C{}'.format(i + 2)] = nc_created[i]
        for i in range(len(so_origin_invoice)):
            sheet['D{}'.format(i + 2)] = so_origin_invoice[i]
        for i in range(len(inv_no_exist)):
            sheet['E{}'.format(i + 2)] = inv_no_exist[i]
        for i in range(len(so_with_refund)):
            sheet['F{}'.format(i + 2)] = so_with_refund[i]

        # Guardar el archivo Excel en disco
        excel_file = 'notas_credito_individuales_amazon_' + today_date.strftime("%Y%m%d") + '.xlsx'
        workbook.save(excel_file)

        # Leer el contenido del archivo Excel
        with open(excel_file, 'rb') as file:
            file_data = file.read()
        file_data_encoded = base64.b64encode(file_data).decode('utf-8')
    except Exception as a:
        print(f"Error: no se pudo crear el archivo de excel: {a}")
    # Correo
    try:
        msg = MIMEMultipart()
        body = '''\
                <html>
                  <head></head>
                  <body>
                    <p>Buenas</p>
                    <p>Hola a todos, espero que estén muy bien. Les comento que acabamos de correr el script de notas de crédito.</p>
                    <p>Adjunto encontrarán el archivo generado por el script en el cual se encuentran las órdenes a las cuales 
                    se les creó una nota de crédito, órdenes que no se les pudo crear una credit_notes, nombre de las notas de crédito 
                    creadas, órdenes que ya contaban con una nota de crédito antes de correr el script y órdenes que tuvieron 
                    algún error, por ejemplo que no existieran dentro de la factura global o no tuvieran una factura creada por la cual se pueda emitir una nota de crédito.</p>
                    </br>
                    <p>Sin más por el momento quedo al pendiente para resolver cualquier duda o comentario.</p>
                    </br>
                    <p>Muchas gracias</p>
                    </br>
                    <p>Un abrazo</p>
                  </body>
                </html>
                '''
        # Define remitente y destinatario
        msg = MIMEMultipart()
        msg['From'] = 'Tech anibal@wonderbrands.co'
        msg['To'] = ', '.join(
            ['anibal@wonderbrands.co', 'rosalba@wonderbrands.co', 'natalia@wonderbrands.co', 'greta@somos-reyes.com',
             'contabilidad@somos-reyes.com', 'alex@wonderbrands.co', 'will@wonderbrands.co'])
        msg['Subject'] = 'Script Automático Amazon - Creación de notas de crédito para facturas individuales'
        # Adjuntar el cuerpo del correo
        msg.attach(MIMEText(body, 'html'))
        # Adjuntar el archivo Excel al mensaje
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(file_data)
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', 'attachment', filename=excel_file)
        msg.attach(attachment)
        print("Enviando correo")
        smtpObj = smtplib.SMTP(smtp_server, smtp_port)
        smtpObj.starttls()
        smtpObj.login(smtp_username, smtp_password)
        smtpObj.sendmail(smtp_username, msg['To'], msg.as_string())
    except Exception as i:
        print(f"Error: no se pudo enviar el correo: {i}")

    print('----------------------------------------------------------------')
    print('PROCESO NC PARA AMAZON COMPLETADO :)')
    print('----------------------------------------------------------------')

    # Cierre de conexiones
    progress_bar.close()
    smtpObj.quit()
    mycursor.close()
    mydb.close()

if __name__ == "__main__":
    reverse_invoice_meli()
    reverse_invoice_amazon()
    print('Listo')
    print('Este arroz ya se coció :)')