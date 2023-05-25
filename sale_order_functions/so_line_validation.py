import time

from flask import Flask, render_template, request, make_response, url_for, session
from os import listdir
from os.path import isfile, join
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

dir_path = os.path.dirname(os.path.realpath(__file__))

#server_url  ='https://wonderbrands.odoo.com'
#db_name = 'wonderbrands-main-4539884'
#username = 'admin'
#password = 'admin123'

dir_path = os.path.dirname(os.path.realpath(__file__))
server_url  ='https://wonderbrands-v3-8038141.dev.odoo.com'
db_name = 'wonderbrands-v3-8038141'
username = 'admin'
password = 'admin123'

print('-------------------------------------------------------')
print('Conectando API Odoo')
print('-------------------------------------------------------')
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(server_url))
uid = common.authenticate(db_name, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(server_url))

print('Conectando a Mysql')
print('-------------------------------------------------------')
# Connect to MySQL database
mydb = mysql.connector.connect(
  host="wonderbrands1.cuwd36ifbz5t.us-east-1.rds.amazonaws.com",
  user="anibal",
  password="Tuy0TEZOcXAwBgtb",
  database="tech"
)
mycursor = mydb.cursor()
#mycursor.execute("SELECT folio, uuid FROM sr_uuid_walmart WHERE id IN %s", [tuple(sales_order_ids)])
#mycursor.execute("SELECT folio, uuid FROM sr_uuid_walmart limit 3")
print(f"Leyendo query")
print('-------------------------------------------------------')
#mycursor.execute("SELECT so.id FROM new_psql_sale_order so LEFT JOIN new_psql_sale_order_line sl ON sl.order_id = so.id WHERE sl.product_id <> 1 AND sl.product_uom_qty > 0 AND sl.qty_delivered = 0 AND so.state = 'done' AND so.id NOT IN (SELECT sale_id FROM new_psql_stock_picking WHERE name LIKE '%/RET/%' AND sale_id <> '' AND state = 'done') AND sl.order_id NOT IN (SELECT sale_id FROM new_psql_stock_picking WHERE sale_id <> '' AND state <> 'done') AND so.create_date BETWEEN '2023-04-01' AND '2023-05-30' AND so.name = 'SO2227431'")
mycursor.execute("SELECT so_name FROM sr_so_global_invoice")
sales_order_records = mycursor.fetchall()
so_name = []
for rec in sales_order_records:
    row = rec[0]
    so_name.append(row)
print(f"Buscando orden de venta")
so_domain = ['name', 'in', so_name]
sale_ids = models.execute_kw(db_name, uid, password,'sale.order', 'search_read', [[so_domain]])

if sale_ids:
    for sale_order in sale_ids:

        sale_id = int(sale_order['id'])
        sale_name = sale_order['name']
        sale_state = sale_order['state']
        sale_date = sale_order['date_order']
        if sale_order['partner_id']:
            sale_partner_id = sale_order['partner_id'][0]
            sale_partner = sale_order['partner_id'][1]
        else:
            sale_partner_id = ''
            sale_partner = ''
        sale_order_line_id = sale_order['order_line']
        print(f'Nombre de la SO: {sale_name} con lineas de orden: {sale_order_line_id}')
        print('-------------------------------------------------------')

        sol_domain = ['id', '=', sale_order_line_id]
        sale_order_line = models.execute_kw(db_name, uid, password, 'sale.order.line', 'search_read', [[sol_domain]])

        for order_line in sale_order_line:
            order_id = order_line['id']
            line_product_uom_qty = order_line['product_uom_qty']
            line_qty_delivered = order_line['qty_delivered']
            line_qty_invoiced = order_line['qty_invoiced']
            line_price_unit = order_line['price_unit']
            line_price_subtotal = order_line['price_subtotal']
            if order_line['product_id']:
                line_product_id = order_line['product_id']
                line_product_name = order_line['product_id']
            else:
                line_product_id = ''
                line_product_name = ''
            sql_data = {
                'order_id': sale_id,
                'name': sale_name,
                'state': sale_state,
                'product_id': line_product_id,
                'product_name': line_product_name,
                'product_uom_qty': line_product_uom_qty,
                'qty_delivered': line_qty_delivered,
                'qty_invoiced': line_qty_invoiced,
                'price_unit': line_price_unit,
                'price_subtotal': line_price_subtotal,
                'partner_id': sale_partner_id,
                'partner_name': sale_partner,
                'date_order': sale_date,
                'order_line': order_id,
            }

            insert_query = "INSERT INTO orders (order_id, name, state, product_id, product_name, product_uom_qty, qty_delivered, qty_invoiced, price_unit, price_subtotal, partner_id, partner_name, date_order, order_line) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"


            upd_so_line_state = models.execute_kw(db_name, uid, password, 'sale.order.line', 'write',[order_id, {'qty_delivered': line_product_uom_qty}])
        print('Orden de venta ', sale_name, ' modificada correctamente')
    print('Proceso finalizado correctamente')
else:
    print(f'No se encontró una orden de venta con ese nombre')

cursor.close()
mydb.close()