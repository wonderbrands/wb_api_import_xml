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
print('BIENVENIDO AL PROCESO DE FACTURACIÓN WALMART')
print('================================================================')
print('SCRIPT DE CREACIÓN DE FACTURAS POR ITEM')
print('================================================================')
today_date = datetime.datetime.now()
dir_path = os.path.dirname(os.path.realpath(__file__))
print('Fecha:' + today_date.strftime("%Y-%m-%d %H:%M:%S"))
#Archivo de configuración - Use config_dev.json si está haciendo pruebas
#Archivo de configuración - Use config.json cuando los cambios vayan a producción
#config_file_name = r'C:\Dev\wb_odoo_external_api\config\config_dev.json'

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
def invoice_create_qty():
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
    print('Este proceso tomará algo de tiempo, le recomendamos ir por un café')
    print('----------------------------------------------------------------')
    mycursor.execute("""select b.name, a.uuid, a.fecha
                        from finance.sr_sat_emitidas a
                        left join somos_reyes.odoo_new_sale_order b
                        on a.folio = b.channel_order_reference
                        WHERE extract(year_month from a.fecha) = 202311
                            and b.channel like '%walmart%' 
                            and b.team_name like '%walmart%'
                            and b.state not in ('draft','sent','cancel')
                            and a.folio in (500234300382,503232800254,455234102524,500234500904,503233500148,869236000069,503233901299,869236500007,503234101897,503234101365,472234401406,869236600913,831236501546,503232500945,870236200333,870236102239,503234001014,504233800886,870236200639,504233900585,503234602410,870236600662,504234300236,504234300957,505232600241,505233100066,871236701460,506234200304,506234100091,506234300445,507232711129,873235719663,507233401077,507234000528,873236301138,507234001407,507234001858,507234201352,873236701573,873236701543,873236801100,508232700890,874236200136,508234000835,874236600851,875235500414,875236200704,509234202191,875236300794,875236700513,875237101855,876235000635,875235500298,510233400017,510233100373,507234500290,510234401865,876237000499,511232401407,877235100622,511233100145,877236202376,511234002481,488232601821,877235100004,877235000712,511234301096,511234501371,512232700513,512233000804,877237000391,512232901487,512233800035,512234100457,512234401535,512234500222,510234101111,512234701246,878236500026,453234500852,879236304167,513233904767,513234003755,513234005502,513234006617,513234008945,879236402504,513234106661,879236602083,879236702479,512234501021,513234402280,513234401565,513234406224,513234503619,513233901013,513233902667,879237001582,501234000134,513234704866,514232405905,514232602876,514232905113,431232801846,514233600749,880236101526,880236202313,514234000325,514233904494,514234103428,880236501177,514234104124,880236504561,514234203206,880236100723,880236604606,880236603602,880236603874,877236501905,880236701942,514234302309,880236200526,880237103489,454233801056,879236001166,881235004043,881235600415,515232900124,515234105976,515234202121,515234201148,470233900791,881236600242,504234301880,881236600301,881236604654,515234204206,515234400319,879236704984,516232701477,516234001984,516234106822,516234303859,882236801977,882236504115,516232800133,879236302126,458234500420,883234900165,514234405089,513232700922,823237102307,517233801933,517234002806,883236403791,517234200059,883236502791,517234105885,517234103764,883236704289,517234401537,883236803186,883236803703,517234300013,517234600965,512234200573,878236600066,883237002751,883236901713,517234702128,883237001733,517234605683,517234701678,884234800017,490234200365,518232403314,518232505534,518232700303,518232502230,518233702023,518233903675,518234004441,884236203042,884236601838,884236602519,518234300955,884236702184,518234402104,518234502515,518234500689,518233901225,884237001839,518233800286,519232401383,519232505320,519232601077,519232605544,519232703540,519232705958,519233101148,885235500691,884236803952,517233700988,885236101609,513234505774,519233800707,885236402937,518234600260,519234101395,885236504537,885236505176,885236501413,519234201479,885236701748,519234304557,519234500230,518234305420,885236900558,885236804241,885236904215,519234504094,885236904560,885236902932,519234503982,519234100093,516232502567,885237102820,519234704903,520232600107,885236101521,520232703111,520232801690,886235301335,520233101100,520233700261,843235001010,520233900728,520233902747,520233900785,518234505141,886236303949,520232804781,513234700078,519234700466,886235003833,520234204177,886236701344,519234300999,520234301477,886236703438,886236600431,886236701268,886236701572,886236800237,520234500568,520234500066,886236500821,885236804188,886236902382,520234403953,516234002664,520234604661,520234700772,520232703255,520234602958,521232400709,500233601816,521232402042,887234800653,887234903113,521232502127,521232601107,887234802085,887235402134,887235203992,521233201167,521233700961,521233700536,887236100605,887236202826,887236203654,887236203337,521233904044,521233906123,521233906154,521233904148,887236402416,887236402747,887236300469,887236200228,887236403417,521234004167,521234006810,521234007690,887236506354,521234200487,521234107751,887236506698,887236200789,521234201488,884236402528,868236401114,520234303870,887236601767,887236601194,521234005553,887236606503,521234206380,521234301103,521234301066,521234300338,883237003259,521234306454,521234400725,520234004937,887236706875,521234401390,521234301816,521234501393,521234502427,521234501370,887236702947,887236903892,521234504535,887236806428,468232900668,887236905544,521234602101,521234600292,887237000134,887237002698,887237002359,887235200265,887237000998,521234505969,521234506833,521234604617,887237000972,887237100168,887237001215,521234505699,887236906722,521234702921,521234703051,521234704501,887236505300,521234705219,887237006183,522232401207,514234606059,886236902904,522232403330,888234804187,888234804297,888234802616,517233100867,521234705585,888234904660,888234905972,522232505957,522232607510,888235101486,522232501018,888235101514,522232804468,887236401327,521232803354,888235301247,521234607022,887237002632,888236100769,887236905487,888236000356,887235102647,522234002902,522234001259,883236202561,522234003632,522234100891,520234405203,888234904353,888236601276,522234201980,522234101221,888236602573,522234204605,888236501909,522234204740,522234206768,522234201369,888236604502,522234302573,888236701937,522234401061,888236904040,888234806483,888236905662,887236706747,522234603543,522234505130,522234606357,888237005663,888237100834,889234802568,523232500414,888237000274,889235100400,523232703358,523232900566,889235500575,523233701041,523233602084,523234002151,889236504917,887236607038,523234201326,889236700511,889236703717,523233100127,889236702381,523234505153,523232504617,521234005571,524232600810,890235002599,890235007243,890235006051,890235106089,890235201037,890235200663,886236703610,880236201203,524232800900,890235300383,524233000582,889235101687,524233902481,890236402762,890236403805,890236301834,519234401954,524234106912,890236606522,890236704249,524234404874,524234404831,890236901506,524234600937,890237004330,859234901051,890237007204,524234704501,884235102620,525232405994,891234806434,889237105567,525232604849,520233902758,525232709615,525232710531,521233000045,891235207653,525233000473,525233002775,525233300235,525233600270,525233801571,525234202035,525234203874,525234203689,815236901494,525234303376,525234300549,891236803215,886236802216,525234600680,891237102906,890236401483,892234900887,526232603190,526232702128,526232803919,526233602652,892236311532,892236402378,892236600417,892236208545,527234000951,892236401374,518234202679,527234600353,528233200270)
                        group by a.uuid
                        order by a.fecha
                        """)
    sales_order_records = mycursor.fetchall()
    xml_dict = {}
    xml_list = []
    inv_list = []
    sales_error_state = []
    sales_no_exist = []
    sales_w_inv = []
    sales_no_xml = []
    sales_mod = []
    inv_names = []
    inv_ids = []
    date_year = '20'
    for row in sales_order_records:
        so_name = row[0] #Del query obtiene el nombre de la SO
        xml_name = row[1] #Del query obtiene el nombre del XML o UUID
        xml_date = date_year + row[2].strftime("%y-%m-%d %H:%M:%S") #Del query obtiene también la fecha de la factura

        if so_name not in xml_dict:
            xml_dict[so_name] = [] #si una SO está repetida le agrega multiples xml y fechas correspondientes

        xml_dict[so_name].append(xml_name) #agrega el nombre del xml a una tabla
        xml_dict[so_name].append(xml_date) #agrega la fecha de factura a una tabla

    for so_order, xml_files in xml_dict.items():
        value_position = 0
        value_position_date = 1
        so_domain = ['name', '=', so_order]
        for xml_ids in so_order[1]:
            xml_list.append(xml_ids)
        #busca la orden de venta y obtiene el nombre y el estado de SO
        sale_ids = models.execute_kw(db_name, uid, password,'sale.order', 'search_read', [[so_domain]])
        try:
            #Si existe una orden de venta
            if sale_ids:
                order_name = sale_ids[0]['name']
                order_state = sale_ids[0]['state']
                #Si la orden está en Done o bloqueada
                if order_state == 'done':
                    invoice_count = sale_ids[0]['invoice_count']
                    #Si la cantidad de facturas es menor a 1, ya que si tiene más podría presentar un retorno
                    if invoice_count < 1:
                        #Crea una factura para cada orden y para cada item
                        #Obtiene los datos necesarios directo de la SO
                        sale_id = int(sale_ids[0]['id'])
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
                        #Busca el sale_order_line para obtener los datos
                        sol_domain = ['id', 'in', sale_order_line_id]
                        sale_order_line = models.execute_kw(db_name, uid, password, 'sale.order.line', 'search_read', [[sol_domain]])
                        for inv_lines in sale_order_line:
                            qty_delivered = round(inv_lines['qty_delivered'])
                            qty_uom = round(inv_lines['product_uom_qty'])
                            #Si al cantidad entregada es diferente de 0
                            if qty_delivered != 0:
                                # Inicia un ciclo para cada item en la columna qty_delivered
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
                                    #Crea la factura con el SKU del line_id
                                    create_inv = models.execute_kw(db_name, uid, password, 'account.move', 'create', [invoice])
                                    #print('La factura de la orden: ', invoice_origin, 'fue creada con ID: ', create_inv)
                                    #Busca la factura para agregar mensaje en el chatter
                                    #print(f"Agregando mensaje a la factura")
                                    search_inv = models.execute_kw(db_name, uid, password, 'account.move', 'search_read', [[['id', '=', create_inv]]])
                                    #agrega el id de la factura creada a una tabla
                                    inv_ids.append(create_inv)
                                    message = {
                                        'body': 'Esta factura fue creada por el equipo de Tech vía API',
                                        'message_type': 'comment',
                                    }
                                    write_msg_inv = models.execute_kw(db_name, uid, password, 'account.move', 'message_post', [create_inv], message)
                                    # Busca el UUID relacionada con la factura
                                    #si existe un archivo XML
                                    if xml_files:
                                        #Obtiene el nombre del XML y la fecha, modifica el nombre del XML y lo pone en mayúsculas
                                        file_name = xml_files[value_position] #utliza la posición que asignamos anteriormente
                                        file_date = xml_files[value_position_date] #utliza la posición que asignamos anteriormente
                                        file_name_mayus = file_name.upper() #Pone en mayúsculas el nombre del XML
                                        invoices_folder = 'G:/Mi unidad/xml_sr_mkp_invoices/Noviembre/' #carpeta en la que se encuentran los xmls
                                        xml_file = file_name + '.xml'
                                        xml_file_path = os.path.join(invoices_folder, xml_file)
                                        with open(xml_file_path, 'rb') as f:
                                            xml_data = f.read()
                                        xml_base64 = base64.b64encode(xml_data).decode('utf-8')
                                        #Define los valores del attachment para agregarl el XML
                                        attachment_data = {
                                            'name': xml_file,
                                            'datas': xml_base64,
                                            'res_model': 'account.move',
                                            'res_id': create_inv,
                                        }
                                        #Busca el id del attachment relacionado a la factura
                                        attachment_ids = models.execute_kw(db_name, uid, password, 'ir.attachment', 'create', [attachment_data])
                                        attachment_id = int(attachment_ids)
                                        values = [{
                                            'move_id': create_inv,
                                            'edi_format_id': 2,
                                            'attachment_id': attachment_id,
                                            'state': 'sent',
                                            'create_uid': 1,
                                            'write_uid': 2,
                                        }]
                                        #Agrega el nombre de la factura a la tabla documentos EDI (solo se ve con debug, conta no la usa)
                                        edi_document = models.execute_kw(db_name, uid, password, 'account.edi.document', 'create', values)
                                        #Valida la factura llamando al botón "Confirmar"
                                        upd_invoice_state = models.execute_kw(db_name, uid, password, 'account.move','action_post', [create_inv])
                                        #Agrega el folio fiscal del XML a la factura y al campo de narration (parche realizado momentaneamente)
                                        upd_folio_fiscal = models.execute_kw(db_name, uid, password, 'account.move','write', [[create_inv], {'l10n_mx_edi_cfdi_uuid': file_name_mayus}])
                                        upd_folio_fiscal_narr = models.execute_kw(db_name, uid, password, 'account.move','write', [[create_inv], {'narration': file_name_mayus}])
                                        # Modifica la fecha de la factura por la del xml y la fecha vencida por "Pago único"
                                        upd_inv_date = models.execute_kw(db_name, uid, password, 'account.move', 'write',[[create_inv], {'invoice_date': file_date}])
                                        upd_inv_date_term = models.execute_kw(db_name, uid, password, 'account.move','write', [[create_inv],{'invoice_payment_term_id': 1}])
                                        #Busca el nombre de la factura una vez publicada meramente infomativo
                                        search_inv_name = models.execute_kw(db_name, uid, password, 'account.move','search_read', [[['id', '=', create_inv]]])
                                        inv_name = search_inv_name[0]['name']
                                        # Busca los asientos de diario relacionados a la factura
                                        #account_line_ids = models.execute_kw(db_name, uid, password, 'account.move.line','search_read', [[['move_id', '=', inv_name]]])
                                        #for each in account_line_ids:
                                        #    move_id = each['id']
                                        #    name_move_id = each['account_id'][1]
                                        #    if name_move_id == '107.05.01 Mercancías Enviadas - No Facturas' or name_move_id == '501-001-001 COSTO DE VENTA':
                                        #        # change_journal_date = models.execute_kw(db_name, uid, password, 'account.move.line', 'write',[[move_id], {'date': inv_date}])
                                        #        change_journal_mat = models.execute_kw(db_name, uid, password,'account.move.line', 'write',[[move_id],{'date_maturity': file_date}])
                                        #    else:
                                        #        change_journal_date = models.execute_kw(db_name, uid, password,'account.move.line', 'write',[[move_id], {'date': file_date}])
                                        #        change_journal_mat = models.execute_kw(db_name, uid, password,'account.move.line', 'write',[[move_id],{'date_maturity': file_date}])
                                        #    print(f"Nombre del Apunte de diario: {name_move_id}")
                                        #posiciones de los array
                                        value_position += 2
                                        value_position_date += 2
                                        sales_mod.append(order_name) #agrega el nombre de la orden a otra tabla
                                        inv_names.append(inv_name) #agrega el nombre que se le asignó a la factura
                                        #print(f"ESTE ES LA POSICION DEL ARRAY: {value_position}")
                                    else:
                                        print(f'La orden: {order_name} no tiene un XML en la carpeta')
                                        sales_no_xml.append(order_name)
                                        continue
                            else:
                                print("Se encontró una factura con cantidad entregada en 0, se tomará en cuenta solo la cantidad")
                                #Inicia un ciclo para cada item en la columna qty_uom
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
                                    create_inv = models.execute_kw(db_name, uid, password, 'account.move', 'create',
                                                                   [invoice])
                                    # Busca la factura para agregar mensaje en el chatter
                                    search_inv = models.execute_kw(db_name, uid, password, 'account.move', 'search_read',
                                                                   [[['id', '=', create_inv]]])
                                    message = {
                                        'body': 'Esta factura fue creada por el equipo de Tech vía API',
                                        'message_type': 'comment',
                                    }
                                    write_msg_inv = models.execute_kw(db_name, uid, password, 'account.move',
                                                                      'message_post', [create_inv], message)
                                    # Busca si hay un UUID relacionada con la factura
                                    if xml_files:
                                        # Obtiene el nombre del XML y la fecha, modifica el nombre del XML y lo pone en mayúsculas
                                        file_name = xml_files[value_position]
                                        file_date = xml_files[value_position_date]
                                        file_name_mayus = file_name.upper()
                                        invoices_folder = 'G:/Mi unidad/xml_sr_mkp_invoices/Noviembre/'
                                        xml_file = file_name + '.xml'
                                        xml_file_path = os.path.join(invoices_folder, xml_file)
                                        with open(xml_file_path, 'rb') as f:
                                            xml_data = f.read()
                                        xml_base64 = base64.b64encode(xml_data).decode('utf-8')
                                        # Define los valores del attachment para agregarl el XML
                                        attachment_data = {
                                            'name': xml_file,
                                            'datas': xml_base64,
                                            'res_model': 'account.move',
                                            'res_id': create_inv,
                                        }
                                        # Busca el id del attachment relacionado a la factura
                                        attachment_ids = models.execute_kw(db_name, uid, password, 'ir.attachment',
                                                                           'create', [attachment_data])
                                        attachment_id = int(attachment_ids)
                                        values = [{
                                            'move_id': create_inv,
                                            'edi_format_id': 2,
                                            'attachment_id': attachment_id,
                                            'state': 'sent',
                                            'create_uid': 1,
                                            'write_uid': 2,
                                        }]
                                        # Agrega el nombre de la factura a la tabla documentos EDI (solo se ve con debug, conta no la usa)
                                        edi_document = models.execute_kw(db_name, uid, password, 'account.edi.document','create', values)
                                        # Valida la factura llamando al botón "Confirmar"
                                        upd_invoice_state = models.execute_kw(db_name, uid, password, 'account.move','action_post', [create_inv])
                                        # Agrega el folio fiscal del XML a la factura y al campo de narration (parche realizado momentaneamente)
                                        upd_folio_fiscal = models.execute_kw(db_name, uid, password, 'account.move','write', [[create_inv], {'l10n_mx_edi_cfdi_uuid': file_name_mayus}])
                                        upd_folio_fiscal_narr = models.execute_kw(db_name, uid, password, 'account.move','write', [[create_inv],{'narration': file_name_mayus}])
                                        # Modifica la fecha de la factura por la del xml y la fecha vencida por "Pago único"
                                        upd_inv_date = models.execute_kw(db_name, uid, password, 'account.move', 'write',[[create_inv], {'invoice_date': file_date}])
                                        upd_inv_date_term = models.execute_kw(db_name, uid, password, 'account.move','write', [[create_inv],{'invoice_payment_term_id': 1}])
                                        # Busca el nombre de la factura una vez publicada meramente infomativo
                                        search_inv_name = models.execute_kw(db_name, uid, password, 'account.move','search_read', [[['id', '=', create_inv]]])
                                        inv_name = search_inv_name[0]['name']
                                        # Busca los asientos de diario relacionados a la factura
                                        #account_line_ids = models.execute_kw(db_name, uid, password, 'account.move.line','search_read', [[['move_id', '=', inv_name]]])
                                        #for each in account_line_ids:
                                        #    move_id = each['id']
                                        #    name_move_id = each['account_id'][1]
                                        #    if name_move_id == '107.05.01 Mercancías Enviadas - No Facturas' or name_move_id == '501-001-001 COSTO DE VENTA':
                                        #        # change_journal_date = models.execute_kw(db_name, uid, password, 'account.move.line', 'write',[[move_id], {'date': inv_date}])
                                        #        change_journal_mat = models.execute_kw(db_name, uid, password,'account.move.line', 'write',[[move_id],{'date_maturity': file_date}])
                                        #    else:
                                        #        change_journal_date = models.execute_kw(db_name, uid, password,'account.move.line', 'write',[[move_id], {'date': file_date}])
                                        #        change_journal_mat = models.execute_kw(db_name, uid, password,'account.move.line', 'write',[[move_id],{'date_maturity': file_date}])
                                        #    print(f"Nombre del Apunte de diario: {name_move_id}")
                                        # posiciones de los array
                                        value_position += 2
                                        value_position_date += 2
                                        sales_mod.append(order_name)
                                        inv_names.append(inv_name)
                                        # print(f"ESTE ES LA POSICION DEL ARRAY: {value_position}")
                                    else:
                                        print(f'La orden: {order_name} no tiene un XML en la carpeta')
                                        continue
                    else:
                        print(f'La orden de venta: {order_name} ya tiene una factura creada')
                        print('----------------------------------------------------------------')
                        sales_w_inv.append(order_name)
                        continue
                else:
                    print(f"Revise el estatus de la orden {order_name} se encuentra en estatus {order_state}")
                    print(f"Por lo que esta orden no puede ser facturada")
                    sales_error_state.append(order_name)
                    continue
            else:
                print(f'El ID de la orden: {order_name} no coincide con ninguna venta en Odoo')
                sales_no_exist.append(order_name)
                continue
        except Exception as e:
            print(f"Error al crear la factura de la orden {order_name}: {e}")

    print('Definiendo correo para contabilidad')
    msg = MIMEMultipart()
    body = '''\
    <html>
      <head></head>
      <body>
        <p>Buenas tardes</p>
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
    sheet['A1'] = 'sales_error_state'
    sheet['B1'] = 'sales_no_exist'
    sheet['C1'] = 'sales_w_inv'
    sheet['D1'] = 'sales_no_xml'
    sheet['E1'] = 'sales_mod'
    sheet['F1'] = 'inv_names'
    sheet['G1'] = 'inv_ids'
    # Agregar los resultados de los arrays
    for i in range(len(sales_error_state)):
        sheet['A{}'.format(i+2)] = sales_error_state[i]
    for i in range(len(sales_no_exist)):
        sheet['B{}'.format(i+2)] = sales_no_exist[i]
    for i in range(len(sales_w_inv)):
        sheet['C{}'.format(i+2)] = sales_w_inv[i]
    for i in range(len(sales_no_xml)):
        sheet['D{}'.format(i+2)] = sales_no_xml[i]
    for i in range(len(sales_mod)):
        sheet['E{}'.format(i+2)] = sales_mod[i]
    for i in range(len(inv_names)):
        sheet['F{}'.format(i+2)] = inv_names[i]
    for i in range(len(inv_ids)):
        sheet['G{}'.format(i+2)] = inv_ids[i]
    # Guardar el archivo Excel en disco
    excel_file = 'ordenes_autofacturadas_' + today_date.strftime("%Y%m%d") + '.xlsx'
    workbook.save(excel_file)
    # Leer el contenido del archivo Excel
    with open(excel_file, 'rb') as file:
        file_data = file.read()
    file_data_encoded = base64.b64encode(file_data).decode('utf-8')
    #Define el encabezado y las direcciones del remitente y destinatarios
    print('Definiendo remitente y destinatarios')
    print('----------------------------------------------------------------')
    msg = MIMEMultipart()
    msg['From'] = 'Tech anibal@wonderbrands.co'
    msg['To'] = ', '.join(['anibal@wonderbrands.co','rosalba@wonderbrands.co','natalia@wonderbrands.co','greta@somos-reyes.com','contabilidad@somos-reyes.com','alex@wonderbrands.co','will@wonderbrands.co'])
    msg['Subject'] = 'Cierre de facturación de órdenes autofacturadas - Walmart'
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
    smtp_username = 'anibal@wonderbrands.co'
    smtp_password = 'iwvrlrxkiydxueer'
    print('Enviando correo con listas de ordenes y facturas')
    print('----------------------------------------------------------------')
    try:
       smtpObj = smtplib.SMTP(smtp_server, smtp_port)
       smtpObj.starttls()
       smtpObj.login(smtp_username, smtp_password)
       smtpObj.sendmail(smtp_username, msg['To'], msg.as_string())
       print("Correo enviado correctamente")
    except Exception as e:
       print(f"Error: no se pudo enviar el correo: {e}")

    print(f"Ordenes que no están en done {sales_error_state}")
    print(f"Ordenes que no existen en Odoo {sales_no_exist}")
    print(f"Ordenes que ya tenían una factura {sales_w_inv}")
    print(f"Ordenes sin XML {sales_no_xml}")
    print(f"Ordenes a las que se les creo factura: {sales_mod}")
    print(f"Nombre de las facturas creadas: {inv_names}")
    print(f"IDs de las facturas creadas: {inv_ids}")

    mycursor.close()
    mydb.close()
    smtpObj.quit()

if __name__ == "__main__":
    invoice_create_qty()