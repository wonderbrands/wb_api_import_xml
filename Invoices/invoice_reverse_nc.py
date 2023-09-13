from flask import Flask, render_template, request, make_response, url_for, session
from email.message import EmailMessage
from email.utils import make_msgid
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from pprint import pprint
from email import encoders
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
print('Bienvenido al proceso de facturación Walmart para notas de crédito')
dir_path = os.path.dirname(os.path.realpath(__file__))
print('----------------------------------------------------------------')
print('Bienvenido al proceso de facturación')
today_date = datetime.datetime.now()
print('Fecha:' + today_date.strftime("%Y%m%d"))
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
print('SCRIPT DE CREACIÓN DE FACTURAS GLOBALES')
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
print('----------------------------------------------------------------')
print('Vaya por un tecito o un café porque este proceso tomará algo de tiempo')
print('----------------------------------------------------------------')
#mycursor.execute("SELECT so_name FROM sr_so_global_invoice")
#mycursor.execute("""SELECT txn_id
#                    FROM bi.sr_master_orders
#                    WHERE out_ym = 202305
#                        AND team_name like '%walmart%'
#                        AND txn_id NOT IN (SELECT b.order_id
#                                            FROM finance.sr_sat_emitidas a
#                                            LEFT JOIN somos_reyes.odoo_master_txns_c b
#                                                ON a.folio = b.marketplace_order_id
#                                            WHERE a.serie = 'PGA'
#                                                AND date(a.fecha) BETWEEN '2023-04-01' AND '2023-05-30'
#                                            GROUP BY b.order_id)
#                    GROUP BY txn_id
#                    ORDER BY out_timestamp_local asc
#                    limit 10""")
excel_file_path = dir_path + '/files/NC/nc_invoices.xlsx'
sale_file = pd.read_excel(excel_file_path, usecols=['invoice_id'])
invoice_records = sale_file['invoice_id'].tolist()
#invoice_records = mycursor.fetchall()
inv_names = []
nc_ids_created = []
print('----------------------------------------------------------------')
print('Creando notas de crédito')
print('Este proceso tomará unos minutos')
print('----------------------------------------------------------------')
try:
    for inv in invoice_records:
        #Consulta el nombre de cada factura y lo agrega a la tabla inv_names
        invoice = models.execute_kw(db_name, uid, password, 'account.move', 'search_read', [[['id', '=', inv]]])
        inv_names.append(invoice[0]['name'])
        if invoice:
            print(f"Número de registros devueltos: {len(invoice)}")
            if len(invoice) == 1:
                inv_name = invoice[0]['name']
                inv_origin = invoice[0]['invoice_origin']
                inv_narration = invoice[0]['narration']
                inv_uuid = inv_narration[3:-4]
                #Crea la nota de crédito mediante la función action_reverse el botón de Odoo
                create_nc = models.execute_kw(db_name, uid, password, 'account.move', 'action_reverse', [inv])
                #Agrega un mensaje específico a la nota de crédito
                message = {
                    'body': f"Esta nota de crédito fue creada a partir de la factura: {inv_name}, de la órden {inv_origin}, con UUID {inv_uuid}, a solicitud del equipo de Contabilidad, por el equipo de Tech.",
                    'message_type': 'comment',
                }
                #write_msg_tech = models.execute_kw(db_name, uid, password, 'account.move', 'message_post', [create_nc],message)
                nc_ids_created.append(create_nc)
            else:
                print(f"Se encontraron múltiples registros para la factura con ID {inv}")
        else:
            print(f"No existe una factura que corresponda al ID {inv}")
            continue
except Exception as e:
   print(f"Error: no se pudo crear la nota de crédito: {e}")
print('----------------------------------------------------------------')
print('Proceso completado')
print('Este arroz ya se coció :)')
print('----------------------------------------------------------------')
print(f"IDs de las notas de crédito creadas: {nc_ids_created}")


mycursor.close()
mydb.close()