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

print('----------------------------------------------------------------')
print('Bienvenido al proceso para creación de notas de crédito')
dir_path = os.path.dirname(os.path.realpath(__file__))
today_date = datetime.datetime.now()
print('Fecha:' + today_date.strftime("%Y-%m-%d %H:%M:%S"))

#Configuración de la API
#server_url  ='https://wonderbrands.odoo.com'
#db_name = 'wonderbrands-main-4539884'
#username = 'admin'
#password = '9Lh5Z0x*bCqV'

server_url  ='https://wonderbrands-vobitest-9523648.dev.odoo.com'
db_name = 'wonderbrands-vobitest-9523648'
username = 'admin'
password = '9Lh5Z0x*bCqV'

print('----------------------------------------------------------------')
print('Conectando API Odoo')
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(server_url))
uid = common.authenticate(db_name, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(server_url))
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
print('----------------------------------------------------------------')
print('Vaya por un tecito o un café porque este proceso tomará algo de tiempo')

mycursor.execute("""SELECT c.name,
                           b.id 'account_move_id',
                           d.payment_date_last_modified
                           /*d.order_id,
                           a.total 'total_factura',
                           a.subtotal 'subtotal_factura',
                           d.refunded_amt 'ml_refunded_amount',
                           b.invoice_partner_display_name 'cliente',
                           b.name,*/
                    FROM finance.sr_sat_emitidas a
                    LEFT JOIN somos_reyes.odoo_new_account_move_aux b
                    ON a.uuid = b.l10n_mx_edi_cfdi_uuid
                    LEFT JOIN somos_reyes.odoo_new_sale_order c
                    ON b.invoice_origin = c.name
                    LEFT JOIN (SELECT order_id, status_detail, pay_status, max(payment_date_last_modified) 'payment_date_last_modified', SUM(paid_amt) 'paid_amt', SUM(refunded_amt) 'refunded_amt' FROM somos_reyes.ml_order_payments WHERE refunded_amt > 0 GROUP BY 1,2,3) d
                    ON c.channel_order_id = d.order_id
                    LEFT JOIN (SELECT distinct invoice_origin FROM somos_reyes.odoo_new_account_move_aux WHERE name like '%RINV%') e
                    ON c.name = e.invoice_origin
                    WHERE d.order_id is not null
                    AND e.invoice_origin is null
                    AND refunded_amt - a.total < 1 AND refunded_amt - a.total > -1""")
#excel_file_path = dir_path + '/files/NC/nc_invoices.xlsx'
#sale_file = pd.read_excel(excel_file_path, usecols=['so_origin'])
#invoice_records = sale_file['so_origin'].tolist()
invoice_records = mycursor.fetchall()
so_no_exist = []
so_w_refund = []
inv_names = []
so_names = []
nc_created = []
print('----------------------------------------------------------------')
print('Creando notas de crédito')
print('Este proceso tomará unos minutos')
print('----------------------------------------------------------------')
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

            if 'out_refund' not in inv_move_types:
                for inv in invoice:
                    inv_id = inv['id'] # ID de la factura
                    inv_name = inv['name'] # Nombre de la factura
                    inv_names.append(inv_name) # Agrega la factura a una tabla
                    inv_origin = inv['invoice_origin'] # Nombre de la SO ligada a la factura
                    so_names.append(inv_origin)
                    #inv_narration = inv['narration']
                    #inv_uuid = inv_narration[3:-4]
                    inv_uuid = inv['l10n_mx_edi_cfdi_uuid'] # Folio fiscal de la factura
                    inv_journal_id = inv['journal_id'][0] #Diario de la factura
                    #Se hace una llamada al wizard de creación de notas de crédito
                    credit_note_wizard = models.execute_kw(db_name, uid, password, 'account.move.reversal', 'create', [{
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
                    progress_bar.update(1)
            else:
                print(f"La órden {inv_origin_name} ya tiene una nota de crédito creada")
                so_w_refund.append(inv_origin_name)
                continue
        else:
            print(f"No hay una factura en la SO {inv_origin_name} por la cual se pueda crear una nota de crédito")
            so_no_exist.append(inv_origin_name)
            continue
except Exception as e:
   print(f"Error: no se pudo crear la nota de crédito: {e}")
print('----------------------------------------------------------------')
print('Proceso completado')
print('Este arroz ya se coció :)')
print('----------------------------------------------------------------')
print('Ordenes')
print(f"SO que no tienen una factura en Odoo: {so_no_exist}")
print(f"SO a las que se les creó nota de crédito: {so_names}")
print(f"SO que ya tienen una nota de crédito: {so_w_refund}")
print('Facturas')
print(f"Facturas a las que se les creó nota de crédito: {inv_names}")

progress_bar.close()

#Cierre de conexiones
mycursor.close()
mydb.close()