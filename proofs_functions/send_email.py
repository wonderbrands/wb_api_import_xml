# from flask import Flask, render_template, request, make_response, url_for, session
# from email.message import EmailMessage
# from email.utils import make_msgid
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from pprint import pprint
from email import encoders
# import time
# import json
# import jsonrpc
# import jsonrpclib
# import random
# import urllib.request
# import getpass
# import http
# import requests
# import logging
# import zipfile
# import socket
import os
import locale
import xmlrpc.client
import base64
import openpyxl
# import xlrd
import pandas as pd
import MySQLdb
import mysql.connector
import smtplib
import ssl
import email
import datetime

#API Configuration
dir_path = os.path.dirname(os.path.realpath(__file__))
print('----------------------------------------------------------------')
print('Bienvenido al proceso de facturación')
today_date = datetime.datetime.now()
print('Fecha:' + today_date.strftime("%Y%m%d"))
print('----------------------------------------------------------------')
print('SCRIPT DE ENVÍO DE CORREO')
print('----------------------------------------------------------------')
print('Obteniendo listas')
print('----------------------------------------------------------------')
inv_ids = [592450, 592451, 592452, 592453, 592454, 592455, 592456, 592457, 592458, 592459, 592460]
mkp_ref = [702235000576,716236901178,350234600546,717235102501,702235000576,351234703312,347234101773,717237103277,351234703312,352234104142,352234300482,350232602407,716235900047,351232401685,718235004113,351234403964,718236702661,352234000811,718235004113,350233901839,350233900449,717236200325,719236800176,352234100414,353232603557,686236901102,353233700844,353233702395,352232400554,353233902929,349234601162,718236601569,354233400027,356232902685,349233400030,720236301156,353234603399,354232401147,354233400027,354233400027,722234904076,354233400027,354233400027,720235000304,354232401147,722234903687,354232403606,354232401147,354232401147,720234802701,355234200712,721237001220,716236502553,355232501428,355234200712,720236500853,350232504356,720236602545,352234202508,351234602252,721237101431,722236200356,354232502380,354232502380,356232500047,719235002693,356234301207,723236602063,356234303188,723235002984,356234501508,720236803280,722236602438,356234203723,357234403846,356234500333,356234600811,720236803280,722236103528,723235303785,354233803282,359234401292,720236201017,354233803282,354233803282,359234401292,691236900716,720236201017,356233703318,354233803282,726234800137,356233703318,720236201017,359233000253,338233901703,726235400539,356234301040,354233803282,717236901619,354232802611,722236601046,714236902329,352234601500,721235001099,351233705735,718235001797,357234104383,723236000710,358232805641,721235001099,359233900057,718236400903,724235305147,357234405023,722234901925,722237102951,359232900920,721235001099,723236804153,723236504662,727236200248,361234501367,722236404136,358234400191,358234400224,361234101671,726236300187,681236500769,722236604583,360234600443,353233703018,357234103214,358234400224,358234400224,700235000487,353234302512,700235000487,106576243975,700235000487,700235000487,700235000487,728236400321,362233900723,721237000391,727235301304,363232601946,356234100447,720234903298,681236500769,726236601039,726236601039,365232700338,726236601039,731235001303,697236501844,356234100059,359234200910,365233900719,732234900096,351233901091,364232400656,361234000800,356233700374,351234602523,714234901092,351234602523,351234602523,720236301822,720236301822,365234401762,731236801815,730236801815,106353414022,723235200071,366234601817,730236801815,106622823296,350234101655,350234101655,356232701890,731236801815,730236801815,730236801815,731236801815,106617799919,730236801815,731235101498,365234401762,731236801815,365233900969,731236801815,736235000775,730236700070,731236401863,730236700070,734237101019,357234101512,364234500419,364234500419,366233800936,734237101019,364234500419,364234500419,360232900096,731236401863,364234500419,366233800936,734235101009,734236400199,730236700070,364234500419,730236200752,360232900096,366233800936,731236401863,364234500419,364234500419,730236700070,364234500419,364234500419,366233800936,724235204125,726236801356,696234900335,371232900994,737234800889,106581530178,359232901399,737236601693,359232700786,357234701553,106648096770,106648096770,106648096770,106648096770,106648096770,371232601375,727235400444,364232602053,364234701871,727235400444,727235400444,727235400444,730235101277,364232602053,730235101235,730235101277,364232701825,730235100661,364232602053,733237000379,730235101277,727235400444,364232701812,725235201104,364232602053,730235100661,364232602053,730235101277,725235201104,730235101277,730235100661,730235100680,730235100661,730235100661,371234401715,739236700330,372234400784,372232701406,372234400784,365233800935,372234400784,737237102388,372234400784,372234400784,738236500664,738236500664,730236702176,737234900708,742235001243,738236600972,738236500664,737234900708,372234100924,737234900708,737234900708,375234501686,369234402023,740236400703,729235500045,372234600170,732237003562,374234401939,740237001625,729235500045,357234604393,374234501016,354233802944,374234401939,371234601146,374234501016,740237001625,729235500045,358234000888,717237003295]
inv_names = ['INV/8202/24800','INV/8202/24799','INV/8202/24798','INV/8202/24797','INV/8202/24796','INV/8202/24795','INV/8202/24794','INV/8202/24793','INV/8202/24792','INV/8202/24791','INV/8202/24790']
# Crear el objeto de mensaje
msg = MIMEMultipart()
body = '''\
<html>
  <head></head>
  <body>
    <p>Buenas noches xxxxxxxxxxxxxxxxxx</p>
    <p>Hola a todos, espero que estén muy bien. Les comento que acabamos de correr el script de autofacturación Walmart.</p>
    <p>Adjunto encontrarán el archivo generado por el script en el cual se encuentran las órdenes a las cuales se les creó una factura, órdenes que no se pudieron facturar, nombre de las facturas creadas y su ids correspondientes.</p>
    </br>
    <p>Sin más por el momento quedo al pendiente para resolver cualquier duda o comentario.</p>
    </br>
    <p>Muchas gracias</p>
    </br>
    <p>Un abrazo</p>
  </body>
</html>
'''
print('Creando archivo Excel')
print('----------------------------------------------------------------')
# Crear el archivo Excel y agregar los nombres de los arrays y los resultados
workbook = openpyxl.Workbook()
sheet = workbook.active
sheet['A1'] = 'inv_ids'
sheet['B1'] = 'inv_names'
sheet['C1'] = 'mkp_ref'
# Agregar los resultados de los arrays
for i in range(len(inv_ids)):
    sheet['A{}'.format(i+2)] = inv_ids[i]
for i in range(len(inv_names)):
    sheet['B{}'.format(i+2)] = inv_names[i]
for i in range(len(mkp_ref)):
    sheet['C{}'.format(i+2)] = mkp_ref[i]
# Guardar el archivo Excel en disco
excel_file = 'balance_general_' + today_date.strftime("%Y%m%d") + '.xlsx'
workbook.save(excel_file)
# Leer el contenido del archivo Excel
with open(excel_file, 'rb') as file:
    file_data = file.read()
file_data_encoded = base64.b64encode(file_data).decode('utf-8')
print('Definiendo remitente y destinatarios')
print('----------------------------------------------------------------')
#Define el encabezado y las direcciones del remitente y destinatarios
msg = MIMEMultipart()
msg['From'] = 'sergio@wonderbrands.co'
#recipients = ['sergiogil.fiein@gmail.com','sergio.gil.guerrero.garcia@gmail.com','sergio@wonderbrands.co'] # sergio.gil.guerrero.garcia lili.men.mor11
msg['To'] = ', '.join(['sergiogil.fiein@gmail.com','sergio.gil.guerrero.garcia@gmail.com','sergio@wonderbrands.co'])
msg['Subject'] = 'Resultados de facturas Walmart GOOD'
# Adjuntar el cuerpo del correo
msg.attach(MIMEText(body, 'html'))
# Adjuntar el archivo Excel al mensaje
attachment = MIMEBase('application', 'octet-stream')
attachment.set_payload(file_data)
encoders.encode_base64(attachment)
attachment.add_header('Content-Disposition', 'attachment', filename=excel_file)
msg.attach(attachment)

#Define variables del servidor de correo
smtp_server = 'smtp.gmail.com'
smtp_port = 587
smtp_username = 'sergio@wonderbrands.co'
smtp_password = 'lwbwgygovuhcyjnk'
print('Enviando correo con listas de ordenes y facturas')
print('----------------------------------------------------------------')
try:
   smtpObj = smtplib.SMTP(smtp_server, smtp_port)
   smtpObj.starttls()
   smtpObj.login(smtp_username, smtp_password)
   # smtpObj.sendmail(smtp_username, msg['To'], msg.as_string())
   smtpObj.send_message(msg)
   print("Correo enviado correctamente")
except Exception as e:
   print(f"Error: no se pudo enviar el correo: {e}")

smtpObj.quit()