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
server_url  ='https://wonderbrands-v3-8038141.dev.odoo.com'
db_name = 'wonderbrands-v3-8038141'
username = 'admin'
password = 'admin123'

print('Conectando API Odoo')
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(server_url))
uid = common.authenticate(db_name, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(server_url))
print(common)
print('Conectando a Mysql')
# Connect to MySQL database
mydb = mysql.connector.connect(
  host="wonderbrands1.cuwd36ifbz5t.us-east-1.rds.amazonaws.com",
  user="anibal",
  password="Tuy0TEZOcXAwBgtb",
  database="tech"
)
mycursor = mydb.cursor()
#mycursor.execute("SELECT folio, uuid FROM sr_uuid_walmart WHERE id IN %s", [tuple(sales_order_ids)])
print('Tomando el Query')
mycursor.execute("SELECT so_name FROM sr_so_global_invoice")
sales_order_records = mycursor.fetchall()
order_ids = []
order_names = []
for order in sales_order_records:
    order_id = models.execute_kw(db_name, uid, password, 'sale.order', 'search', [[['name', '=', order]]])
    order_ids.append(order_id[0])
    order_names.append(order[0])

try:
    #Se crea el cuerpo de la factura con los campos necesarios
    so_domain = ['id', 'in', order_ids]
    print('----------------------------------------------------------------')
    print(f"Se creará la factura global para las siguientes SO: {order_ids}")
    print('----------------------------------------------------------------')
    print('Definiendo valores de la factura global')
    invoice_vals = {
        'ref': '',
        'move_type': 'out_invoice',
        'partner_id': 140530,
        'invoice_origin': ', '.join(order_names),
        'invoice_line_ids': [],
    }
    # Consultamos a sale.order para obtener los campos requeridos de cada orden de venta
    order = models.execute_kw(db_name, uid, password, 'sale.order', 'search_read', [[so_domain]])
    for order_line in order:
        order_line_id = order_line['order_line']
        for line in order_line_id:
            sale_order_line = models.execute_kw(db_name, uid, password, 'sale.order.line', 'search_read', [[['id', '=', line]]])
            invoice_line_vals = {
                'display_type': sale_order_line[0]['display_type'],
                'sequence': int(sale_order_line[0]['sequence']),
                'name': sale_order_line[0]['name'],
                'product_uom_id': sale_order_line[0]['product_uom'][0],
                'product_id': sale_order_line[0]['product_id'][0],
                'quantity': sale_order_line[0]['qty_delivered'],
                'discount': sale_order_line[0]['discount'],
                'price_unit': sale_order_line[0]['price_unit'],
                'tax_ids': [(6, 0, [sale_order_line[0]['tax_id'][0]])],
                'analytic_tag_ids': [(6, 0, sale_order_line[0]['analytic_tag_ids'])],
                'sale_line_ids': [(4, line)],
            }
            invoice_vals['invoice_line_ids'].append((0, 0, invoice_line_vals))
    invoice_id = models.execute_kw(db_name, uid, password, 'account.move', 'create', [invoice_vals])
    print('----------------------------------------------------------------')
    print(f"Se creó la factura correctamente")
    print(f"El ID de la factura es el siguiente: {invoice_id}")
    print('----------------------------------------------------------------')
except Exception as e:
    print(f"Error al crear la factura con error: {e}")

mycursor.close()
mydb.close()