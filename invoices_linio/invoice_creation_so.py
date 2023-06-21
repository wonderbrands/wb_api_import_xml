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
#server_url  ='https://wonderbrands-v3-8443304.dev.odoo.com'
#db_name = 'wonderbrands-v3-8443304'
#username = 'admin'
#password = 'admin123'

server_url  ='https://wonderbrands-v1-8647463.dev.odoo.com'
db_name = 'wonderbrands-v1-8647463'
username = 'admin'
password = 'admin123'

print('----------------------------------------------------------------')
print('SCRIPT DE CREACIÓN DE FACTURAS POR SO')
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
print(f"Leyendo query")
print('----------------------------------------------------------------')
print('Este proceso tomará algo de tiempo, le recomendamos ir por un café')
print('----------------------------------------------------------------')
mycursor.execute("""SELECT a.order_id, c.uuid, c.fecha
                    FROM somos_reyes.odoo_master_txns_c a
                    LEFT JOIN tech.sr_linio_invoices_dito b
                        ON a.marketplace_order_id = b.orden
                    LEFT JOIN finance.sr_sat_emitidas c
                        ON b.serie = c.folio
                    WHERE a.order_id IN ('SO2165634','SO2170659','SO2181932','SO2182963','SO2184510','SO2184902','SO2186495','SO2187818','SO2188517','SO2188609','SO2189911','SO2189917','SO2189955','SO2190435','SO2191034','SO2191089','SO2191244','SO2191522','SO2191666','SO2192038','SO2192070','SO2192150','SO2192151','SO2192290','SO2192801','SO2193443','SO2193446','SO2193933','SO2194191','SO2194230','SO2194332','SO2194348','SO2194442','SO2194662','SO2194836','SO2194839','SO2194840','SO2194995','SO2195693','SO2195728','SO2195729','SO2195912','SO2196450','SO2196515','SO2196806','SO2196834','SO2197003','SO2197245','SO2197260','SO2197407','SO2197953','SO2198242','SO2198604','SO2198653','SO2198749','SO2199695','SO2200149','SO2200284','SO2200733','SO2201132','SO2201535','SO2203083','SO2203306','SO2203684','SO2203928','SO2204169','SO2204351','SO2205043','SO2205408','SO2206227','SO2206498','SO2206658','SO2206896','SO2207431','SO2207706','SO2208889','SO2208981','SO2209215','SO2209963','SO2210749','SO2210754','SO2211442','SO2212288','SO2212707','SO2212748','SO2212749','SO2212770','SO2212960','SO2213205','SO2213570','SO2213935','SO2214021','SO2214186','SO2214233','SO2214241','SO2214278','SO2214785','SO2215066','SO2215230','SO2215737','SO2215756','SO2215781','SO2216096','SO2216392','SO2216550','SO2216820','SO2217008','SO2217037','SO2217146','SO2217292','SO2217854','SO2218044','SO2218235','SO2218366','SO2218633','SO2218960','SO2219166','SO2219824','SO2220153','SO2220276','SO2220480','SO2220527','SO2220706','SO2221236','SO2221476','SO2221672','SO2221881','SO2221905','SO2222148','SO2222203','SO2222210','SO2222509','SO2222510','SO2222629','SO2222730','SO2222894','SO2222947','SO2222990','SO2223298','SO2223351','SO2223743','SO2224201','SO2224396','SO2224417','SO2224513','SO2224897','SO2225044','SO2225095','SO2225107','SO2225179','SO2225267','SO2225348','SO2225448','SO2225523','SO2225598','SO2225652','SO2225748','SO2226613','SO2226686','SO2227576','SO2227693','SO2228058','SO2228087','SO2228166','SO2228310','SO2228432','SO2228755','SO2228831','SO2228938','SO2228957','SO2228961','SO2229249','SO2229250','SO2229309','SO2229402','SO2229547','SO2229628','SO2229851','SO2229862','SO2230087','SO2230286','SO2230405','SO2230607','SO2230746','SO2230920','SO2230971','SO2231067','SO2231077','SO2231094','SO2231095','SO2231290','SO2232533','SO2233004','SO2233300','SO2233346','SO2233429','SO2233693','SO2234018','SO2234276','SO2234587','SO2234839','SO2234947','SO2235130','SO2235150','SO2235186','SO2235233','SO2235243','SO2235248','SO2235498','SO2235509','SO2235581','SO2235817','SO2235859','SO2236153','SO2236697','SO2236715','SO2236862','SO2236881','SO2236965','SO2237543','SO2237912','SO2238290','SO2238446','SO2238566','SO2238741','SO2238940','SO2239162','SO2239387','SO2240251','SO2240322','SO2241481','SO2241841','SO2241942','SO2242028','SO2242090','SO2242557','SO2242600','SO2243448','SO2243573','SO2244075','SO2245036','SO2245043','SO2245447','SO2245851','SO2246063','SO2246231','SO2246303','SO2246468','SO2247067','SO2247304','SO2247385','SO2247454','SO2247477','SO2247726','SO2247828','SO2248077','SO2248086','SO2248928','SO2249303','SO2249975','SO2249989','SO2250318','SO2250762','SO2250791','SO2250963','SO2251259','SO2251331','SO2251437','SO2251510','SO2251646','SO2251861','SO2252603','SO2253212','SO2253474','SO2253501','SO2253563','SO2253580','SO2253883','SO2254152','SO2254592','SO2254658','SO2255076','SO2255112','SO2255472','SO2255541','SO2255579','SO2256284','SO2256286','SO2256604','SO2256843','SO2257087','SO2257190','SO2257544','SO2257721','SO2257849','SO2258170','SO2258254','SO2258265','SO2258317','SO2258348','SO2258494','SO2258508','SO2258578','SO2258587','SO2258604','SO2258675','SO2258902','SO2258912','SO2259147','SO2259203','SO2259258','SO2259420','SO2259433','SO2259576','SO2259577','SO2259684','SO2259704','SO2259987','SO2260488','SO2260562','SO2261149','SO2261382','SO2261588','SO2261738','SO2261797','SO2261869','SO2262278','SO2262389','SO2262401','SO2262597','SO2262723','SO2263071','SO2263252','SO2263277','SO2263314','SO2263463','SO2263494','SO2263579','SO2263817','SO2264041','SO2264054','SO2264092','SO2264250','SO2264306','SO2264359','SO2264363','SO2264372','SO2264398','SO2264417','SO2264429','SO2264453','SO2264454','SO2264463','SO2264479','SO2264526','SO2264594','SO2264598','SO2264670','SO2264680','SO2264739','SO2264751','SO2264781','SO2264832','SO2264870','SO2264962','SO2265079','SO2265081','SO2265176','SO2265194','SO2265273','SO2265326','SO2265333','SO2265370','SO2265428','SO2265585','SO2265762','SO2265828','SO2265830','SO2265911','SO2265916','SO2265935','SO2266049','SO2266060','SO2266118','SO2266167','SO2266216','SO2266232','SO2266293','SO2266449','SO2266473','SO2266602','SO2266783','SO2266832','SO2266880','SO2266939','SO2266945','SO2266946','SO2266966','SO2267023','SO2267034','SO2267096','SO2267339','SO2267394','SO2267399','SO2267416','SO2267434','SO2267473','SO2267480','SO2267512','SO2267542','SO2267549','SO2267659','SO2267680','SO2267739','SO2267938','SO2267981','SO2268020','SO2268021','SO2268030','SO2268081','SO2268093','SO2268142','SO2268156','SO2268193','SO2268194','SO2268220','SO2268277','SO2268561','SO2268566','SO2268642','SO2268834','SO2268865','SO2268881','SO2268968','SO2269241','SO2269328','SO2269330','SO2269456','SO2269501','SO2269630','SO2269676','SO2269745','SO2269878','SO2269962','SO2269964','SO2269965','SO2269995','SO2270120','SO2270277','SO2270401','SO2270441','SO2270809','SO2270888','SO2271290','SO2271394','SO2271482','SO2271517','SO2271539','SO2271751','SO2271816','SO2271817','SO2271839','SO2272152','SO2272315','SO2272449','SO2272794','SO2272796','SO2272905','SO2272984','SO2273030','SO2273164','SO2273165','SO2273166','SO2273167','SO2273168','SO2273294','SO2273576','SO2273587','SO2273674','SO2273851','SO2273853','SO2273854','SO2273879','SO2273971','SO2274106','SO2274121','SO2274199','SO2274252','SO2274433','SO2274507','SO2274854','SO2274959','SO2275097','SO2275255','SO2275345','SO2275347','SO2275473','SO2275585','SO2276109','SO2276132','SO2276260','SO2276476','SO2276634','SO2276690','SO2276717','SO2276858','SO2276990','SO2276992','SO2276994','SO2277060','SO2277136','SO2277232','SO2277405','SO2277425','SO2277786','SO2277824','SO2277904','SO2277928','SO2277959','SO2278132','SO2278145','SO2278150','SO2278172','SO2278188','SO2278424','SO2278461','SO2278523','SO2278674','SO2278703','SO2278834','SO2278840','SO2278898','SO2279161','SO2279181','SO2279291','SO2279314','SO2279403','SO2279730','SO2279732','SO2279784','SO2279871','SO2279969','SO2280220','SO2280430','SO2280500','SO2280634','SO2280908','SO2280935','SO2280937','SO2280951','SO2280991','SO2281120','SO2281215','SO2281238','SO2281249','SO2281286','SO2281406','SO2281441','SO2281546','SO2281624','SO2281659','SO2281803','SO2282121','SO2282138','SO2282214','SO2282224','SO2282378','SO2282390','SO2282524','SO2282538','SO2282550','SO2282560','SO2282581','SO2282582','SO2282767','SO2282775','SO2282802','SO2282817','SO2282884','SO2282954','SO2282981','SO2283063','SO2283094','SO2283456','SO2283467','SO2283486','SO2283528','SO2283608','SO2283712','SO2283774','SO2283839','SO2284019','SO2284049','SO2284072','SO2284180','SO2284254','SO2284258','SO2284309','SO2284347','SO2284451','SO2284453','SO2284471','SO2284568','SO2284643','SO2284690','SO2284729','SO2284748','SO2284749','SO2284780','SO2284841','SO2284856','SO2284920','SO2285046','SO2285077','SO2285113','SO2285258','SO2285454','SO2285510','SO2285599','SO2285622','SO2285630','SO2285831','SO2285871','SO2285884','SO2285885','SO2285934','SO2286002','SO2286004','SO2286036','SO2286096','SO2286244','SO2286461','SO2286551','SO2286609','SO2286828','SO2286863','SO2287049','SO2287097','SO2287136','SO2287149','SO2287227','SO2287228','SO2287293','SO2287389','SO2287403','SO2287440','SO2287477','SO2287522','SO2287606','SO2287741','SO2287884','SO2287926','SO2287927','SO2287952','SO2288089','SO2288104','SO2288121','SO2288154','SO2288218','SO2288324','SO2288340','SO2288373','SO2288385','SO2288610','SO2288642','SO2288653','SO2288683','SO2288710','SO2288723','SO2288824','SO2288825','SO2288852','SO2288877','SO2288880','SO2288903','SO2288977','SO2289002','SO2289003','SO2289021','SO2289022','SO2289260','SO2289261','SO2289445','SO2289453','SO2289506','SO2289572','SO2289595','SO2289747','SO2289832','SO2290034','SO2290050','SO2290124','SO2290181','SO2290194','SO2290294','SO2290368','SO2290386','SO2290432','SO2290435','SO2290457','SO2290599','SO2290694','SO2290779','SO2290939','SO2291029','SO2291122','SO2291311','SO2291367','SO2291405','SO2291482','SO2291483','SO2291484','SO2291527','SO2291571','SO2291615','SO2291627','SO2291667','SO2291669','SO2291736','SO2291768','SO2291778','SO2291786','SO2291821','SO2291862','SO2291898','SO2291942','SO2291943','SO2292042','SO2292109','SO2292110','SO2292111','SO2292112','SO2292148','SO2292176','SO2292234','SO2292251','SO2292323','SO2292324','SO2292325','SO2292349','SO2292454','SO2292455','SO2292519','SO2292520','SO2292529','SO2292648','SO2293356','SO2294034','SO2294196','SO2294327','SO2294611','SO2294742','SO2295207','SO2295694','SO2296097','SO2296856','SO2296883','SO2297143','SO2300168')
                    ORDER BY c.fecha
                    limit 100""")
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
#Ciclo que ordena las SO, los UUIDS y las fechas en un diccionario
for row in sales_order_records:
    so_name = row[0]
    xml_name = row[1]
    xml_date = date_year + row[2].strftime("%y-%m-%d %H:%M:%S")

    if so_name not in xml_dict:
        xml_dict[so_name] = []

    xml_dict[so_name].append(xml_name)
    xml_dict[so_name].append(xml_date)
for so_order, xml_files in xml_dict.items():
    value_position = 0
    value_position_date = 1
    so_domain = ['name', '=', so_order]
    for xml_ids in so_order[1]:
        xml_list.append(xml_ids)
    sale_ids = models.execute_kw(db_name, uid, password,'sale.order', 'search_read', [[so_domain]])
    try:
        if sale_ids:
            #Obtiene los datos principales de la factura
            order_name = sale_ids[0]['name']
            order_state = sale_ids[0]['state']
            print(f"Orden de venta encontrada en el sistema")
            so_inv_count = sale_ids[0]['invoice_count']
            if order_state == 'done':
                if so_inv_count < 1:
                    print(f"Nombre de la orden de venta {order_name}")
                    print(f"Estado de la orden de venta: {order_state}")
                    print("Definiendo valores de la factura")
                    invoice_id = []
                    sale_id = int(sale_ids[0]['id'])
                    sale_order_line_id = sale_ids[0]['order_line']
                    print(f"ID de lineas de orden de venta: {sale_order_line_id}")
                    #Define los valores de la factura en un diccionario
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
                    #Busca el id del la tabla order_line en el modelo sale.order.line y los agrega al diccionario de la factura
                    line_domain = ['id', 'in', sale_order_line_id]
                    sale_order_line = models.execute_kw(db_name, uid, password, 'sale.order.line', 'search_read', [[line_domain]])
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
                    #Crea la factura con los valores previamente obtenidos
                    create_inv = models.execute_kw(db_name, uid, password, 'account.move', 'create', [invoice])
                    # Agrega un mensaje a la factura para identificar las facturas que se hacen por API
                    print('La factura de la orden: ', order_name, 'fue creada con ID: ', create_inv)
                    inv_ids.append(create_inv)
                    print("Agregando mensaje a la factura")
                    message = {
                        'body': 'Esta factura fue creada por el equipo de Tech vía API',
                        'message_type': 'comment',
                    }
                    write_msg_tech = models.execute_kw(db_name, uid, password, 'account.move', 'message_post', [create_inv],message)
                    if xml_files:
                        #Determina la posición del uuid y la fecha de factura en el diccionario xml_dict
                        file_name = xml_files[value_position]
                        file_date = xml_files[value_position_date]
                        file_name_mayus = file_name.upper()
                        print(f"AGREGANDO ARCHIVO XML A LA FACTURA")
                        invoices_folder = 'G:/Mi unidad/xml_linio_invoices/'
                        print(f"El xml {file_name} será agregado a la factura")
                        #Busca el XML en la carpeta previamente definida
                        xml_file = file_name + '.xml'
                        xml_file_path = os.path.join(invoices_folder, xml_file)
                        with open(xml_file_path, 'rb') as f:
                            xml_data = f.read()
                        xml_base64 = base64.b64encode(xml_data).decode('utf-8')
                        #Crea una lista para agregar el archivo XML al attachment de la factura
                        attachment_data = {
                            'name': xml_file,
                            'datas': xml_base64,
                            'res_model': 'account.move',
                            'res_id': create_inv,
                        }
                        #Agrega la lista al attachment
                        attachment_ids = models.execute_kw(db_name, uid, password, 'ir.attachment', 'create',[attachment_data])
                        attachment_id = int(attachment_ids)
                        #Crea una lista para agregar el nombre del UUID a la tabla EDI document (esta tabla solo se puede ver en modo Debug en Odoo)
                        values = [{
                            'move_id': create_inv,
                            'edi_format_id': 2,
                            'attachment_id': attachment_id,
                            'state': 'sent',
                            'create_uid': 1,
                            'write_uid': 2,
                        }]
                        print('AGREGANDO REGISTRO XML A LA TABLA DOCUMENTOS EDI')
                        edi_document = models.execute_kw(db_name, uid, password, 'account.edi.document', 'create',values)
                        print('Valores para la tabla Documentos EDI: ', values)
                        print('Registro account.edi.document creado')
                        print("Actualizando estado de la factura")
                        #Se actualiza el estado de la factura a done llamando al botón "Confirmar"
                        #upd_invoice_state = models.execute_kw(db_name, uid, password, 'account.move', 'action_post',[create_inv])
                        print('Se publica la factura: ', create_inv)
                        print(f"Se agrega el folio fiscal: {file_name_mayus}")
                        #Actuaiza el campo de folio fiscal
                        upd_folio_fiscal = models.execute_kw(db_name, uid, password, 'account.move', 'write',[[create_inv], {'l10n_mx_edi_cfdi_uuid': file_name_mayus}])
                        # Parche momentaneo ya que el folio fiscal no funciona por ahora
                        upd_folio_fiscal_narr = models.execute_kw(db_name, uid, password, 'account.move', 'write',[[create_inv], {'narration': file_name_mayus}])
                        # Actualiza Fecha de factura
                        print(f"Se Modifica la fecha de factura: {file_date}")
                        upd_inv_date = models.execute_kw(db_name, uid, password, 'account.move', 'write',[[create_inv], {'invoice_date': file_date}])
                        #Busca el nombre de la factura como referencia una vez publicada
                        search_inv_name = models.execute_kw(db_name, uid, password, 'account.move', 'search_read',[[['id', '=', create_inv]]])
                        inv_name = search_inv_name[0]['name']
                        #Posiciones de los array por si las SO tienen más de un UUID
                        value_position += 2
                        value_position_date += 2
                        sales_mod.append(order_name)
                        inv_names.append(inv_name)
                        print('----------------------------------------------------------------')
                    else:
                        print(f'La orden: {order_name} no tiene un XML en la carpeta')
                        sales_no_xml.append(order_name)
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
            print(f'El ID de la orden: {so_order} no coincide con ninguna venta en Odoo')
            sales_no_exist.append(so_order)
            continue
    except Exception as e:
        print(f"Error al crear la factura de la orden {so_order}: {e}")

print('PROCESO DE FACTURACIÓN POR ORDEN DE VENTA FINALIZADO')
print(f"Ordenes que no están en done {sales_error_state}")
print(f"Ordenes que no existen en Odoo {sales_no_exist}")
print(f"Ordenes que ya tenían una factura {sales_w_inv}")
print(f"Ordenes sin XML {sales_no_xml}")
print(f"Ordenes a las que se les creo factura: {sales_mod}")
print(f"Nombre de las facturas creadas: {inv_names}")
print(f"IDs de las facturas creadas: {inv_ids}")


mycursor.close()
mydb.close()
