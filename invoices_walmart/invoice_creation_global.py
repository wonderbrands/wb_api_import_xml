import time

from flask import Flask, render_template, request, make_response, url_for, session
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
server_url  ='https://wonderbrands-v3-8443304.dev.odoo.com'
db_name = 'wonderbrands-v3-8443304'
username = 'admin'
password = 'admin123'

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
#mycursor.execute("SELECT folio, uuid FROM sr_uuid_walmart WHERE id IN %s", [tuple(sales_order_ids)])
print(f"Leyendo query")
print('----------------------------------------------------------------')
print('Vaya por un tecito o un café porque este proceso tomará algo de tiempo')
print('----------------------------------------------------------------')
#mycursor.execute("SELECT so_name FROM sr_so_global_invoice")
mycursor.execute("""SELECT txn_id
                    FROM bi.sr_master_orders	
                    WHERE out_ym = 202305	
                        AND team_name like '%walmart%'
                        AND txn_id NOT IN (SELECT b.order_id
                                            FROM finance.sr_sat_emitidas a
                                            LEFT JOIN somos_reyes.odoo_master_txns_c b
                                                ON a.folio = b.marketplace_order_id
                                            WHERE a.serie = 'PGA'
                                                AND date(a.fecha) BETWEEN '2023-04-01' AND '2023-05-30'
                                            GROUP BY b.order_id)
                    GROUP BY txn_id	
                    ORDER BY out_timestamp_local asc
                    limit 10""")
sales_order_records = mycursor.fetchall()
order_names = []
try:
    for order in sales_order_records:
        order_id = models.execute_kw(db_name, uid, password, 'sale.order', 'search_read', [[['name', '=', order]]])
        order_names.append(order_id[0]['name'])

        #Se crea el cuerpo de la factura con los campos necesarios
    #so_domain = ['name', 'in', order_names]
    print('----------------------------------------------------------------')
    print(f"Se creará la factura global para las siguientes SO: {order_names}")
    print('----------------------------------------------------------------')
    print('Definiendo valores de la factura global')
    print('----------------------------------------------------------------')
    print('Vaya por otro tecito u otro café porque este proceso tomará unos minutos')
    print('----------------------------------------------------------------')
    invoice_vals = {
        'ref': '',
        'move_type': 'out_invoice',
        'partner_id': 140530,
        'invoice_origin': ', '.join(order_names),
        'invoice_line_ids': [],
    }
    # Consultamos a sale.order para obtener los campos requeridos de cada orden de venta
    for sale_order in order_names:
        so_domain = ['name', '=', sale_order]
        order = models.execute_kw(db_name, uid, password, 'sale.order', 'search_read', [[so_domain]])

        #print(f"Orden de venta encontrada")
        order_line_id = order[0]['order_line']
        order_name = order[0]['name']
        order_state = order[0]['state']
        order_inv_count = order[0]['invoice_count']
        if order_state == 'done':
            if order_inv_count < 1:
                #for line in order_line_id:
                #print(f"Tomando las lineas de la orden")
                sale_order_line = models.execute_kw(db_name, uid, password, 'sale.order.line', 'search_read', [[['id', '=', order_line_id]]])
                for line in sale_order_line:
                    line_id = line['id']
                    invoice_line_vals = {
                        'display_type': line['display_type'],
                        'sequence': int(line['sequence']),
                        'name': line['name'],
                        'product_uom_id': line['product_uom'][0],
                        'product_id': line['product_id'][0],
                        'quantity': line['qty_delivered'],
                        'discount': line['discount'],
                        'price_unit': line['price_unit'],
                        'tax_ids': [(6, 0, [line['tax_id'][0]])],
                        'analytic_tag_ids': [(6, 0, line['analytic_tag_ids'])],
                        'sale_line_ids': [(4, line_id)],
                    }
                    invoice_vals['invoice_line_ids'].append((0, 0, invoice_line_vals))
            else:
                print(f"La factura {order_name} ya tiene una orden creada")
                continue
        else:
            print(f"La orden de venta {order_name} se encuentra en estatus {order_state}")
            print(f"Por lo que esta orden no puede ser facturada")
            continue
    invoice_id = models.execute_kw(db_name, uid, password, 'account.move', 'create', [invoice_vals])
    print(f"Agregando mensaje a la factura")
    #Mensaje con ordenes de venta como referencia en el chatter
    message_so = {
        'body': order_names,
        'message_type': 'comment',
    }
    write_msg_inv = models.execute_kw(db_name, uid, password, 'account.move', 'message_post', [invoice_id], message_so)
    #Mensaje de creación por API
    message = {
        'body': 'Esta factura fue creada por el equipo de Tech vía API',
        'message_type': 'comment',
    }
    write_msg_tech = models.execute_kw(db_name, uid, password, 'account.move', 'message_post', [invoice_id], message)
    print('----------------------------------------------------------------')
    print(f"Se creó la factura correctamente")
    print(f"El ID de la factura es el siguiente: {invoice_id}")
    print('----------------------------------------------------------------')
except Exception as e:
    print(f"Error al crear la factura con error: {e}")

mycursor.close()
mydb.close()