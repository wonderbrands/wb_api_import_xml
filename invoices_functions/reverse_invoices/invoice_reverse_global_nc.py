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

    mycursor.execute("""SELECT c.name, b.id 'account_move_id', b.name/*d.order_id, a.total, a.subtotal, d.refunded_amt,b.invoice_partner_display_name*/
                        FROM finance.sr_sat_emitidas a
                        LEFT JOIN somos_reyes.odoo_new_account_move_aux b
                        ON a.uuid = b.l10n_mx_edi_cfdi_uuid
                        LEFT JOIN somos_reyes.odoo_new_sale_order c
                        ON SUBSTRING_INDEX(SUBSTRING_INDEX(invoice_ids, ']', 1), '[', -1) = b.id
                        LEFT JOIN (SELECT order_id, status_detail, pay_status, SUM(paid_amt) 'paid_amt', SUM(refunded_amt) 'refunded_amt' 
                                    FROM somos_reyes.ml_order_payments 
                                    WHERE refunded_amt > 0 
                                    GROUP BY 1,2,3) d
                        ON c.channel_order_id = d.order_id
                        LEFT JOIN (SELECT distinct invoice_origin 
                                    FROM somos_reyes.odoo_new_account_move_aux 
                                    WHERE name like '%RINV%') e
                        ON c.name = e.invoice_origin
                        WHERE d.order_id is not null
                            AND e.invoice_origin is null
                            #AND refunded_amt - a.total > 1 OR refunded_amt - a.total < -1
                            AND invoice_partner_display_name = 'PÚBLICO EN GENERAL'
                            and b.name = 'INV/8202/05535'
                            limit 1""")
    invoice_records = mycursor.fetchall()
    so_no_exist = []
    so_w_refund = []
    inv_names = []
    so_names = []
    nc_created = []
    so_no_exist_in_invoice = []
    print('----------------------------------------------------------------')
    print('Creando notas de crédito')
    print('Este proceso tomará unos minutos')
    #invoice_records = [('SO2479520', '821764', 'INV/8202/40340'), ('SO2474777', '821764', 'INV/8202/40340')]
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
                    inv_uuid = inv['l10n_mx_edi_cfdi_uuid']  # Folio fiscal de la factura
                    inv_usage = inv['l10n_mx_edi_usage']  # Folio fiscal de la factura
                    inv_journal_id = inv['journal_id'][0]
                    if inv_origin_name in inv['invoice_origin']:
                        #Busca la órden de venta
                        sale_order = models.execute_kw(db_name, uid, password, 'sale.order', 'search_read', [[['name', '=', inv_origin_name]]])[0]
                        # Obtiene los datos necesarios directo de la SO
                        sale_id = sale_order['id']
                        sale_name = sale_order['name']
                        #Busca el order line correspondiente de la orden de venta
                        sale_line_id = models.execute_kw(db_name, uid, password, 'sale.order.line', 'search_read', [[['order_id', '=', sale_id]]])
                        #Define los valores de la nota de crédito
                        inv_int = int(inv_id)
                        sale_int = int(sale_id)
                        refund_vals = {
                            'ref': f'Reversión de: {inv_name}',
                            'journal_id': inv_journal_id,
                            'invoice_origin': sale_name,
                            'payment_reference': inv_name,
                            'invoice_date': datetime.datetime.now().strftime('%Y-%m-%d'),
                            # Puedes ajustar la fecha según tus necesidades
                            'partner_id': inv['partner_id'][0],
                            'l10n_mx_edi_usage': inv_usage,
                            'reversed_entry_id': inv_int,
                            'move_type': 'out_refund',  # Este campo indica que es una nota de crédito
                            'invoice_line_ids': []
                        }
                        for lines in sale_line_id:
                            nc_lines = {'product_id': lines['product_id'][0],
                                        'quantity': lines['product_uom_qty'],
                                        'name': lines['name'],  # Puedes ajustar esto según tus necesidades
                                        'price_unit': lines['price_unit'],
                                        }
                            refund_vals['invoice_line_ids'].append((0, 0, nc_lines))
                        #Crea la nota de crédito
                        create_nc = models.execute_kw(db_name, uid, password, 'account.move', 'create', [refund_vals])
                        #Actualiza la nota de crédito
                        upd_nc_move = models.execute_kw(db_name, uid, password, 'account.move', 'write',[[create_nc], {'reversal_move_id': sale_int}])
                        #Agrega mensaje al Attachment de la nota de crédito
                        message = {
                            'body': f"Esta nota de crédito fue creada a partir de la factura: {inv_name}, de la órden {sale_name}, con folio fiscal {inv_uuid}, a solicitud del equipo de Contabilidad, por el equipo de Tech mediante API.",
                            'message_type': 'comment',
                        }
                        write_msg_inv = models.execute_kw(db_name, uid, password, 'account.move', 'message_post',[create_nc], message)
                        nc = models.execute_kw(db_name, uid, password, 'account.move', 'search_read',[[['id', '=', create_nc]]])
                        print('listo man')
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

    print('----------------------------------------------------------------')
    print('Proceso completado')
    print('Este arroz ya se coció :)')
    print('----------------------------------------------------------------')

    # Cierre de conexiones
    progress_bar.close()
    #smtpObj.quit()
    mycursor.close()
    mydb.close()

if __name__ == "__main__":
    reverse_invoice_global()