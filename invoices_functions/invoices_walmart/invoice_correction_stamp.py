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
print('BIENVENIDO AL PROCESO DE CORRECCIÓN DE FACTURAS')
print('================================================================')
today_date = datetime.datetime.now()
dir_path = os.path.dirname(os.path.realpath(__file__))
print('Fecha:' + today_date.strftime("%Y-%m-%d %H:%M:%S"))
#Archivo de configuración - Use config_dev.json si está haciendo pruebas
#Archivo de configuración - Use config.json cuando los cambios vayan a producción
folder = r'C:\Dev\wb_odoo_external_api'
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
def correction_invoice():
    odoo_keys = get_odoo_access()
    psql_keys = get_psql_access()
    email_keys = get_email_access()
    # odoo
    server_url = odoo_keys['odoourl']
    db_name = odoo_keys['odoodb']
    username = odoo_keys['odoouser']
    password = odoo_keys['odoopassword']

    print('----------------------------------------------------------------')
    print('Conectando API Odoo')
    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(server_url))
    uid = common.authenticate(db_name, username, password, {})
    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(server_url))
    print('Conexión con Odoo establecida')
    print('----------------------------------------------------------------')
    print('Vaya por un tecito o un café porque este proceso tomará algo de tiempo')
    print('----------------------------------------------------------------')
    excel_file_path = r'C:\Dev\wb_odoo_external_api\invoices_functions\files\corrections\correction_file.xlsx'
    file = pd.read_excel(excel_file_path, usecols=['ID'])
    invoice_records = file['ID'].tolist()

    for each in invoice_records:
        int_id = int(each)
        inv_name = models.execute_kw(db_name, uid, password, 'account.move', 'search_read',[[['id', '=', int_id]]])[0]
        attachment = models.execute_kw(db_name, uid, password, 'ir.attachment', 'search_read', [[['res_id', '=', int_id]]])[0]
        edi_document_ids = models.execute_kw(db_name, uid, password, 'account.edi.document', 'search_read',[[['move_id', '=', int_id]]])

        attachment_id = attachment['id']
        xml_name = attachment['name']
        xml_name = xml_name[:-4]
        values = [{
            'move_id': int_id,
            'edi_format_id': 2,
            'attachment_id': attachment_id,
            'state': 'sent',
            'create_uid': 1,
            'write_uid': 2,
        }]
        edi_document = models.execute_kw(db_name, uid, password, 'account.edi.document', 'create', values)
        print("hola2")

if __name__ == "__main__":
    correction_invoice()
    end_time = datetime.datetime.now()
    duration = end_time - today_date
    print(f'Duración del script: {duration}')
    print('Listo')
    print('Este arroz ya se coció :)')