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
print('Conectando a Mysql - Tech')
# Connect to MySQL database
mydb = mysql.connector.connect(
  host="wonderbrands1.cuwd36ifbz5t.us-east-1.rds.amazonaws.com",
  user="anibal",
  password="Tuy0TEZOcXAwBgtb",
  database="tech"
)
mycursor = mydb.cursor()
print('Tomando el Query')
mycursor.execute("SELECT so_name, uuid FROM sr_so_invoice")
sales_order_records = mycursor.fetchall()
mkp_reference = []
for reference in sales_order_records:
    row = reference
    mkp_reference.append(row)

for channel_order_reference in mkp_reference:
    order_reference = channel_order_reference[0]
    uuid_reference = channel_order_reference[1]
    so_domain = ['name', '=', order_reference]
    sale_ids = models.execute_kw(db_name, uid, password,'sale.order', 'search_read', [[so_domain]])
    so_name = sale_ids[0]['name']
    if sale_ids:
        # Create invoice for sales order
        invoice_id = []
        sale_id = int(sale_ids[0]['id'])
        sale_order_line_id = sale_ids[0]['order_line']
        # Define the invoice values
        invoice = {
            'ref': '',
            'move_type': 'out_invoice',
            'currency_id': sale_ids[0]['currency_id'][0],
            'narration': sale_ids[0]['note'],
            'campaign_id': False,
            'medium_id': sale_ids[0]['medium_id'],
            'source_id': sale_ids[0]['source_id'],
            'user_id': sale_ids[0]['user_id'][0],
            'invoice_user_id': sale_ids[0]['user_id'][0],
            'team_id': sale_ids[0]['team_id'][0],
            'partner_id': sale_ids[0]['partner_id'][0],
            'partner_shipping_id': sale_ids[0]['partner_shipping_id'][0],
            'fiscal_position_id': sale_ids[0]['fiscal_position_id'],
            'partner_bank_id': 1,
            'journal_id': 1,  # company comes from the journal
            'invoice_origin': sale_ids[0]['name'],
            'invoice_payment_term_id': sale_ids[0]['payment_term_id'],
            'payment_reference': sale_ids[0]['reference'],
            'transaction_ids': [(6, 0, sale_ids[0]['transaction_ids'])],
            'invoice_line_ids': [],
            'company_id': 1,
        }
        # Call to sale order line to get order line data
        sol_domain = ['id', 'in', sale_order_line_id]
        sale_order_line = models.execute_kw(db_name, uid, password, 'sale.order.line', 'search_read', [[sol_domain]])
        for inv_lines in sale_order_line:
            line_id = inv_lines['id']
            invoice_lines = {'display_type': inv_lines['display_type'],
                             'sequence': inv_lines['sequence'],
                             'name': inv_lines['name'],
                             'product_id': inv_lines['product_id'][0],
                             'product_uom_id': inv_lines['product_uom'][0],
                             'quantity': inv_lines['product_qty'],
                             'discount': inv_lines['discount'],
                             'price_unit': inv_lines['price_unit'],
                             'tax_ids': [(6, 0, [inv_lines['tax_id'][0]])],
                             'analytic_tag_ids': [(6, 0, inv_lines['analytic_tag_ids'])],
                             'sale_line_ids': [(4, line_id)],
                             }
            invoice['invoice_line_ids'].append((0,0, invoice_lines))
        create_inv = models.execute_kw(db_name, uid, password, 'account.move', 'create', [invoice])
        print('La factura de la orden: ', so_name, 'fue creada con ID: ', create_inv)
    else:
        print('El ID de la orden de MP: ', so_name,'no coincide con ninguna venta en Odoo')
        pass

    mydb.close()
