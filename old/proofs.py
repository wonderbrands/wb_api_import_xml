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
server_url  ='https://wonderbrands-v3-8038141.dev.odoo.com'
db_name = 'wonderbrands-v3-8038141'
username = 'admin'
password = 'admin123'

print('Conectando API Odoo')
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(server_url))
uid = common.authenticate(db_name, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(server_url))
print(common)

today = datetime.today().date()
start_date = (today + relativedelta(day=1)).strftime('%Y-%m-%d')
end_date = (today + relativedelta(day=1, months=+1)).strftime('%Y-%m-%d')

print(today)
print(start_date)
print(end_date)
