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
mycursor.execute("SELECT folio, uuid FROM sr_uuid_walmart limit 3")
sales_order_records = mycursor.fetchall()
mkp_reference = []
for reference in sales_order_records:
    row = reference
    mkp_reference.append(row)
#mkp_reference = ['SO2213460', 'SO2213461', 'SO2213462']
for channel_order_reference in mkp_reference:
    order_reference = channel_order_reference
    order_reference = channel_order_reference[0]
    uuid_reference = channel_order_reference[1]
    #uuid_reference = channel_order_reference
    #so_domain = ['name', '=', order_reference]
    so_domain = ['channel_order_reference', '=', order_reference]
    #so_domain = ['name', '=', 'SO2213460']
    #sale_ids = models.execute_kw(db_name, uid, password,'sale.order', 'search_read', [[so_domain]], {'fields': ['id', 'name','partner_id', 'order_line']})
    sale_ids = models.execute_kw(db_name, uid, password,'sale.order', 'search_read', [[so_domain]])
    so_name = sale_ids[0]['name']
    try:
        if sale_ids:
            invoice_count = sale_ids[0]['invoice_count']
            if invoice_count < 1:
                # Create invoice for sales order
                invoice_id = []
                sale_id = int(sale_ids[0]['id'])
                #for sale_order in sale_ids:
                #sale_id = int(sale_order['id'])
                currency_id = sale_ids[0]['currency_id'][0]
                narration = sale_ids[0]['note']
                campaign_id = False
                medium_id = sale_ids[0]['medium_id']
                source_id = sale_ids[0]['source_id']
                user_id = sale_ids[0]['user_id'][0]
                invoice_user_id = sale_ids[0]['user_id'][0]
                team_id = sale_ids[0]['team_id'][0]
                partner_id = sale_ids[0]['partner_id'][0]
                partner_shipping_id = sale_ids[0]['partner_shipping_id'][0]
                fiscal_position_id = sale_ids[0]['fiscal_position_id']
                partner_bank_id = 1
                journal_id = 1
                invoice_origin = sale_ids[0]['name']
                invoice_payment_term_id = sale_ids[0]['payment_term_id']
                payment_reference = sale_ids[0]['reference']
                transaction_ids = sale_ids[0]['transaction_ids']
                company_id = 1
                sale_order_line_id = sale_ids[0]['order_line']
                #sale_order_line_change =  sale_ids[0]['order_line'][0]
                # Call to sale order line to get order line data
                sol_domain = ['id', 'in', sale_order_line_id]
                sale_order_line = models.execute_kw(db_name, uid, password, 'sale.order.line', 'search_read', [[sol_domain]])
                for inv_lines in sale_order_line:
                    qty_delivered = round(inv_lines['qty_delivered'])
                    qty_uom = round(inv_lines['product_uom_qty'])
                    if qty_delivered != 0:
                        for qty in range(qty_delivered):
                            invoice = {
                                'ref': '',
                                'move_type': 'out_invoice',
                                'currency_id': currency_id,
                                'narration': narration,
                                'campaign_id': campaign_id,
                                'medium_id': medium_id,
                                'source_id': source_id,
                                'user_id': user_id,
                                'invoice_user_id': invoice_user_id,
                                'team_id': team_id,
                                'partner_id': partner_id,
                                'partner_shipping_id': partner_shipping_id,
                                'fiscal_position_id': fiscal_position_id,
                                'partner_bank_id': partner_bank_id,
                                'journal_id': journal_id,  # company comes from the journal
                                'invoice_origin': invoice_origin,
                                'invoice_payment_term_id': invoice_payment_term_id,
                                'payment_reference': payment_reference,
                                'transaction_ids': [(6, 0, transaction_ids)],
                                'invoice_line_ids': [],
                                'company_id': company_id,
                            }
                            #line_id = sale_order_line[0]['id']
                            line_id = inv_lines['id']
                            invoice_lines = {'display_type': inv_lines['display_type'],
                                             'sequence': inv_lines['sequence'],
                                             'name': inv_lines['name'],
                                             'product_id': inv_lines['product_id'][0],
                                             'product_uom_id': inv_lines['product_uom'][0],
                                             #'quantity': sale_order_line[0]['product_qty'],
                                             'quantity': 1,
                                             'discount': inv_lines['discount'],
                                             'price_unit': inv_lines['price_unit'],
                                             'tax_ids': [(6, 0, [inv_lines['tax_id'][0]])],
                                             'analytic_tag_ids': [(6, 0, inv_lines['analytic_tag_ids'])],
                                             'sale_line_ids': [(4, line_id)],
                                             }
                            invoice['invoice_line_ids'].append((0, 0, invoice_lines))
                            create_inv = models.execute_kw(db_name, uid, password, 'account.move', 'create', [invoice])
                            print('La factura de la orden: ', invoice_origin, 'fue creada con ID: ', create_inv)
                            #Search invoice in account_move
                            search_inv = models.execute_kw(db_name, uid, password, 'account.move', 'search_read', [[['id', '=', create_inv]]])
                            message = {
                                'body': 'Esta factura fue creada por el equipo de Tech vía API',
                                'message_type': 'comment',
                            }
                            write_msg_inv = models.execute_kw(db_name, uid, password, 'account.move', 'message_post', [create_inv], message)
                    else:
                        for qty in range(qty_uom):
                            invoice = {
                                'ref': '',
                                'move_type': 'out_invoice',
                                'currency_id': currency_id,
                                'narration': narration,
                                'campaign_id': campaign_id,
                                'medium_id': medium_id,
                                'source_id': source_id,
                                'user_id': user_id,
                                'invoice_user_id': invoice_user_id,
                                'team_id': team_id,
                                'partner_id': partner_id,
                                'partner_shipping_id': partner_shipping_id,
                                'fiscal_position_id': fiscal_position_id,
                                'partner_bank_id': partner_bank_id,
                                'journal_id': journal_id,  # company comes from the journal
                                'invoice_origin': invoice_origin,
                                'invoice_payment_term_id': invoice_payment_term_id,
                                'payment_reference': payment_reference,
                                'transaction_ids': [(6, 0, transaction_ids)],
                                'invoice_line_ids': [],
                                'company_id': company_id,
                            }
                            # line_id = sale_order_line[0]['id']
                            line_id = inv_lines['id']
                            invoice_lines = {'display_type': inv_lines['display_type'],
                                             'sequence': inv_lines['sequence'],
                                             'name': inv_lines['name'],
                                             'product_id': inv_lines['product_id'][0],
                                             'product_uom_id': inv_lines['product_uom'][0],
                                             # 'quantity': sale_order_line[0]['product_qty'],
                                             'quantity': 1,
                                             'discount': inv_lines['discount'],
                                             'price_unit': inv_lines['price_unit'],
                                             'tax_ids': [(6, 0, [inv_lines['tax_id'][0]])],
                                             'analytic_tag_ids': [(6, 0, inv_lines['analytic_tag_ids'])],
                                             'sale_line_ids': [(4, line_id)],
                                             }
                            invoice['invoice_line_ids'].append((0, 0, invoice_lines))
                            create_inv = models.execute_kw(db_name, uid, password, 'account.move', 'create', [invoice])
                            print('La factura de la orden: ', invoice_origin, 'fue creada con ID: ', create_inv)
                            # Search invoice in account_move
                            search_inv = models.execute_kw(db_name, uid, password, 'account.move', 'search_read',
                                                           [[['id', '=', create_inv]]])
                            message = {
                                'body': 'Esta factura fue creada por el equipo de Tech vía API',
                                'message_type': 'comment',
                            }
                            write_msg_inv = models.execute_kw(db_name, uid, password, 'account.move', 'message_post',
                                                              [create_inv], message)
            else:
                print('La orden con mkp reference: ', order_reference,'ya tiene una factura creada')
        else:
            print('El ID de la orden de MP: ', invoice_origin,'no coincide con ninguna venta en Odoo')
            pass
    except Exception as e:
        print(f"Error al crear la factura de la orden {so_name}: {e}")

mydb.close()
