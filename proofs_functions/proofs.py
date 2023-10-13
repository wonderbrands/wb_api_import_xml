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
print('----------------------------------------------------------------')

config_file_name = r'C:\Dev\wb_odoo_external_api\config_dev.json'

def get_odoo_access():
    with open(config_file_name, 'r') as config_file:
        config = json.load(config_file)

    return config['odoo']
def get_psql_access():
    with open(config_file_name, 'r') as config_file:
        config = json.load(config_file)

    return config['psql']
def autoinvoice():
    # Obtener credenciales
    odoo_keys = get_odoo_access()
    psql_keys = get_psql_access()

    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(odoo_keys['odoourl']))
    uid = common.authenticate(odoo_keys['odoodb'], odoo_keys['odoouser'], odoo_keys['odoopassword'], {})
    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(odoo_keys['odoourl']))

    print(common)

    mydb = mysql.connector.connect(
        host=psql_keys['dbhost'],
        user=psql_keys['dbuser'],
        password=psql_keys['dbpassword'],
        database=psql_keys['database']
    )
    mycursor = mydb.cursor()

    mycursor.execute("""SELECT folio FROM finance.sr_sat_emitidas WHERE serie = 'PGA' limit 10""")
    invoice_records = mycursor.fetchall()
    for each in invoice_records:
        print(f"Folio de venta: {each}")

if __name__ == "__main__":
    autoinvoice()