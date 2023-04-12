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

dir_path = os.path.dirname(os.path.realpath(__file__))

server_url  ='https://wonderbrands.odoo.com'
db_name = 'wonderbrands-main-4539884'
username = 'admin'
password = 'admin123'

print('Conexión con la API de Odoo')
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(server_url))
uid = common.authenticate(db_name, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(server_url))

# Define the path to the Excel file containing the orders and Read Excel file
excel_file_path = dir_path + '/files/Orden de venta.xlsx'
sale_file = pd.read_excel(excel_file_path, usecols=['ID'])
sale_id_file = sale_file['ID'].tolist()

so_domain = ['id', 'in', sale_id_file]
sale_ids = models.execute_kw(db_name, uid, password,'sale.order', 'search_read', [[so_domain]])

if sale_ids:
    for sale_order in sale_ids:

        sale_id = int(sale_order['id'])
        sale_order_line_id = int(sale_order['order_line'][0])
        print('Id de SO: ', sale_id, ' con lineas de SO', sale_order_line_id)

        sol_domain = ['id', '=', sale_order_line_id]
        sale_order_line = models.execute_kw(db_name, uid, password, 'sale.order.line', 'search_read', [[sol_domain]], {'fields': ['id','product_uom_qty', 'qty_delivered']})

        order_id = sale_order_line[0]['id']
        product_uom_qty = sale_order_line[0]['product_uom_qty']
        qty_delivered = sale_order_line[0]['qty_delivered']

        upd_so_line_state = models.execute_kw(db_name, uid, password, 'sale.order.line', 'write',[order_id, {'qty_delivered': product_uom_qty}])
        print('Orden de venta ', sale_id, ' modificada correctamente')
    print('Proceso finalizado correctamente')