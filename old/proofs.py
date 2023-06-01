import time

from flask import Flask, render_template, request, make_response, url_for, session
from datetime import date, datetime, timedelta
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
server_url  ='https://wonderbrands-v3-8462601.dev.odoo.com'
db_name = 'wonderbrands-v3-8462601'
username = 'admin'
password = 'admin123'

print('Conectando API Odoo')
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(server_url))
uid = common.authenticate(db_name, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(server_url))
print(common)

id = 521346
search_inv = models.execute_kw(db_name, uid, password, 'account.move', 'search_read', [[['id', '=', id]]])

folio = search_inv[0]['l10n_mx_edi_cfdi_uuid']

file_name = '0e485015-bd53-5e45-8127-ce671e185e02'



file_name_mayus = file_name.upper()

print(f"XML en minusculas {file_name}")
print(f"XML en mayusculas {file_name_mayus}")
upd_folio_fiscal = models.execute_kw(db_name, uid, password, 'account.move','write', [[521346], {'l10n_mx_edi_cfdi_uuid': file_name_mayus}])

fecha = datetime.now()

print(fecha)