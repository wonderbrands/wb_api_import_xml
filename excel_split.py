from flask import Flask, render_template, request, make_response, url_for, session
from email.message import EmailMessage
from email.utils import make_msgid
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from pprint import pprint
from email import encoders
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

#API Configuration
dir_path = os.path.dirname(os.path.realpath(__file__))
print('----------------------------------------------------------------')
print('Bienvenido al proceso de facturación')
today_date = datetime.datetime.now()
print('Fecha:' + today_date.strftime("%Y%m%d"))
print('----------------------------------------------------------------')
print('SCRIPT DE DIVISION DE EXCEL')
print('----------------------------------------------------------------')
print('Obteniendo listas')
print('----------------------------------------------------------------')

so_names = ['SO2326357','SO2334472','SO2340122','SO2345382','SO2347396','SO2350016','SO2350258','SO2353121','SO2353619','SO2355711','SO2356895','SO2358633','SO2362827','SO2363930','SO2364707','SO2365089','SO2365225','SO2366257','SO2368712','SO2369623','SO2369936','SO2369947','SO2369952','SO2370358','SO2370864','SO2370865','SO2370932','SO2371089','SO2371145','SO2371184','SO2371191','SO2371341','SO2371478','SO2371527','SO2371670','SO2371805','SO2371833','SO2371834','SO2371836','SO2371872','SO2371885','SO2371990','SO2372009','SO2372010','SO2372025','SO2372044','SO2372045','SO2372046','SO2372064','SO2372080','SO2372081','SO2372092','SO2372094','SO2372102','SO2372114','SO2372115','SO2372129','SO2372150','SO2372151','SO2372190','SO2372207','SO2372220','SO2372234','SO2372235','SO2372249','SO2372264','SO2372270','SO2372284','SO2372305','SO2372315','SO2372316','SO2372327','SO2372328','SO2372338','SO2372382','SO2372392','SO2372427','SO2372445','SO2372448','SO2372454','SO2372470','SO2372516','SO2372536','SO2372566','SO2372570','SO2372612','SO2372629','SO2372630','SO2372640','SO2372653','SO2372670','SO2372677','SO2372678','SO2372689','SO2372739','SO2372771','SO2372799','SO2372800','SO2372808','SO2372809','SO2372816','SO2372818','SO2372869','SO2372935','SO2373009','SO2373044','SO2373045','SO2373046','SO2373071','SO2373111','SO2373166','SO2373188','SO2373200','SO2373201','SO2373243','SO2373259','SO2373260','SO2373261','SO2373276','SO2373277','SO2373289','SO2373310','SO2373352','SO2373386','SO2373404','SO2373405','SO2373406','SO2373469','SO2373479','SO2373497','SO2373500','SO2373518','SO2373538','SO2373547','SO2373548','SO2373559','SO2373587','SO2373597','SO2373606','SO2373614','SO2373624','SO2373633','SO2373687','SO2373688','SO2373698','SO2373707','SO2373714','SO2373720','SO2373721','SO2373754','SO2373765','SO2373767','SO2373780','SO2373783','SO2373787','SO2373815','SO2373827','SO2373831','SO2373844','SO2373858','SO2373875','SO2373884','SO2373891','SO2373906','SO2373927','SO2373986','SO2374024','SO2374025','SO2374038','SO2374069','SO2374070','SO2374078','SO2374087','SO2374088','SO2374100','SO2374108','SO2374119','SO2374136','SO2374147','SO2374149','SO2374200','SO2374201','SO2374202','SO2374207','SO2374302','SO2374305','SO2374311','SO2374314','SO2374329','SO2374339','SO2374346','SO2374349','SO2374350','SO2374366','SO2374367','SO2374381','SO2374395','SO2374409','SO2374411','SO2374421','SO2374423','SO2374428','SO2374443','SO2374457','SO2374459','SO2374489','SO2374491','SO2374498','SO2374502','SO2374526','SO2374551','SO2374562','SO2374566','SO2374576','SO2374582','SO2374624','SO2374634','SO2374638','SO2374650','SO2374655','SO2374671','SO2374672','SO2374685','SO2374694','SO2374713','SO2374739','SO2374768','SO2374769','SO2374774','SO2374803','SO2374807','SO2374832','SO2374833','SO2374851','SO2374852','SO2374855','SO2374891','SO2374905','SO2374923','SO2374939','SO2374940','SO2374941','SO2374967','SO2374996','SO2375019','SO2375035','SO2375055','SO2375056','SO2375062','SO2375079','SO2375104','SO2375123','SO2375144','SO2375158','SO2375162','SO2375198','SO2375203','SO2375221','SO2375223','SO2375226','SO2375227','SO2375253','SO2375283','SO2375303','SO2375315','SO2375316','SO2375331','SO2375344','SO2375345','SO2375366','SO2375376','SO2375377','SO2375386','SO2375422','SO2375453','SO2375478','SO2375504','SO2375523','SO2375527','SO2375543','SO2375559','SO2375572','SO2375582','SO2375613','SO2375633','SO2375645','SO2375662','SO2375669','SO2375703','SO2375707','SO2375782','SO2375868','SO2375881','SO2375922','SO2375944','SO2375946','SO2375952','SO2375953','SO2375974','SO2375975','SO2376001','SO2376003','SO2376038','SO2376039','SO2376047','SO2376059','SO2376078','SO2376107','SO2376108','SO2376134','SO2376171','SO2376198','SO2376199','SO2376238','SO2283240','SO2283239','SO2283230','SO2283217','SO2283202','SO2283185','SO2283168','SO2283166','SO2283150','SO2283149','SO2283148','SO2283147','SO2283124','SO2283123','SO2283122','SO2283121','SO2283120','SO2283119','SO2283118','SO2283101','SO2283100','SO2283099','SO2283076','SO2283058','SO2283035','SO2283034','SO2283024','SO2283023','SO2283022','SO2283005','SO2283004','SO2282985','SO2282984','SO2282982','SO2282979','SO2282977','SO2282967','SO2282950','SO2282936','SO2282934','SO2282903','SO2282894','SO2282893','SO2282877','SO2282860','SO2282859','SO2282858','SO2282832','SO2282831','SO2282830','SO2282829','SO2282797','SO2282796','SO2282781','SO2282780','SO2282779','SO2282755','SO2282741','SO2282740','SO2282739','SO2282707','SO2282677','SO2282669','SO2282668','SO2281526','SO2282637','SO2282636','SO2282608','SO2282607','SO2282590','SO2282557','SO2282532','SO2282523','SO2282522','SO2282489','SO2282480','SO2282479','SO2282478','SO2282461','SO2282451','SO2282449','SO2280403','SO2282420','SO2282399','SO2282398','SO2282367','SO2282366','SO2282357','SO2282326','SO2282306','SO2266027','SO2270582','SO2282293','SO2282280','SO2282273','SO2282252','SO2282250','SO2282234','SO2282229','SO2282228','SO2282181','SO2282165','SO2282147','SO2282146','SO2282145','SO2282132','SO2282131','SO2282128','SO2282095','SO2282036','SO2282025','SO2281994','SO2281982','SO2281976','SO2281954','SO2281953','SO2281936','SO2281935','SO2281934','SO2281906','SO2281895','SO2281879','SO2281878','SO2281857','SO2281856','SO2281845','SO2281844','SO2281842','SO2281786','SO2281785','SO2281774','SO2281755','SO2281720','SO2281696','SO2281695','SO2281665','SO2281631','SO2281629','SO2281595','SO2281594','SO2281593','SO2281591','SO2281578','SO2281569','SO2281541','SO2281528','SO2281525','SO2281503','SO2281502','SO2281501','SO2281500','SO2281499','SO2281485','SO2281484','SO2281476','SO2281447','SO2281377','SO2281369','SO2281368','SO2281353','SO2281352','SO2281305','SO2281289','SO2281288','SO2281264','SO2281263','SO2281221','SO2281220','SO2281188','SO2281182','SO2281181','SO2281180','SO2281135','SO2281134','SO2281125','SO2281124','SO2281123','SO2281106','SO2281105','SO2281104','SO2281095','SO2281085','SO2281059','SO2281024','SO2281014','SO2281013','SO2281011','SO2281010','SO2281009','SO2281008','SO2281007','SO2280615','SO2280983','SO2280969','SO2280940','SO2280939','SO2280929','SO2280915','SO2280914','SO2280913','SO2280889','SO2280888','SO2280887','SO2280886','SO2280884','SO2280859','SO2280858','SO2280857','SO2280856','SO2280837','SO2280836','SO2280835','SO2280834','SO2280806','SO2280805','SO2280794','SO2280793','SO2280747','SO2280746','SO2280744','SO2280709','SO2280706','SO2280690','SO2280689','SO2280652','SO2280651','SO2280633','SO2280617','SO2280616','SO2280614','SO2280555','SO2280498','SO2280476','SO2280406','SO2280404','SO2280402','SO2280371','SO2280369','SO2280341','SO2279026','SO2280305','SO2280300','SO2280299','SO2280275','SO2280274','SO2280260','SO2280251','SO2280250','SO2280249','SO2280239','SO2280212','SO2280191','SO2280190','SO2280169','SO2280161','SO2280160','SO2280132','SO2280106','SO2280069','SO2280048','SO2280023','SO2280010','SO2280009','SO2279997','SO2279936','SO2279894','SO2279893','SO2279891','SO2267058','SO2272714','SO2279867','SO2279865','SO2279850','SO2279846','SO2279845','SO2279835','SO2279833','SO2279832','SO2279824','SO2279819','SO2279818','SO2279810','SO2279797','SO2279796','SO2279780','SO2279761','SO2279743','SO2279736','SO2279709','SO2279708','SO2279707','SO2279642','SO2279641','SO2279609','SO2279562','SO2279561','SO2279543','SO2279534','SO2279508','SO2279490','SO2279489','SO2279456','SO2279455','SO2279454','SO2279443','SO2279442','SO2279430','SO2279398','SO2279382','SO2279380','SO2279350','SO2279339','SO2279275','SO2279273','SO2279260','SO2279259','SO2279233','SO2279219','SO2279218','SO2279217','SO2279197','SO2279169','SO2279144','SO2279143','SO2279142','SO2279134','SO2279133','SO2279132','SO2279131','SO2279130','SO2279080','SO2279079','SO2279058','SO2279057','SO2279025','SO2278998','SO2278997','SO2278966','SO2278965','SO2278948','SO2278917','SO2278916','SO2278915','SO2278914','SO2278894','SO2278886','SO2278884','SO2278883','SO2278868','SO2278857','SO2278818','SO2278817','SO2278782','SO2278771','SO2278770','SO2278760','SO2278742','SO2278732','SO2278671','SO2278670','SO2278637','SO2278636','SO2278619','SO2278585','SO2278567','SO2278565','SO2278552','SO2278550','SO2278527','SO2278517','SO2278516','SO2278515','SO2278491','SO2278490','SO2278471','SO2278470','SO2278469','SO2278468','SO2278449','SO2278407','SO2278406','SO2278348','SO2278396','SO2278365','SO2278364','SO2278353','SO2278352','SO2278351','SO2278350','SO2278349','SO2278309','SO2278293','SO2278292','SO2278291','SO2278262','SO2278261','SO2278260','SO2278259','SO2278206','SO2278205','SO2278182','SO2278181','SO2278180','SO2278179','SO2278178','SO2278156','SO2278121','SO2278031','SO2278030','SO2278014','SO2278001','SO2277966','SO2277948','SO2277947','SO2277946','SO2277920','SO2277919','SO2277892','SO2277822','SO2277811','SO2277810','SO2277785','SO2277784','SO2277750','SO2277724','SO2277723','SO2277712','SO2277711','SO2277709','SO2277708','SO2277707','SO2277684','SO2277606','SO2277586','SO2277585','SO2277576','SO2277560','SO2277539','SO2277538','SO2277521','SO2277505','SO2276247','SO2277471','SO2277460','SO2277409','SO2277408','SO2277407','SO2277387','SO2277386','SO2277380','SO2277364','SO2277363','SO2277352','SO2277351','SO2277337','SO2277316','SO2277304','SO2277291','SO2277285','SO2277260','SO2277241','SO2270406','SO2265368','SO2270378','SO2277231','SO2277222','SO2277218','SO2277159','SO2277145','SO2277144','SO2277123','SO2277100','SO2277071','SO2277069','SO2277064','SO2276985','SO2276984','SO2276983','SO2276981','SO2276955','SO2276939','SO2276936','SO2276886','SO2276885','SO2276884','SO2276863','SO2276862','SO2276861','SO2276817','SO2276816','SO2276793','SO2276792','SO2276791','SO2276790','SO2276762','SO2276745','SO2276743','SO2276723','SO2276722','SO2276721','SO2276679','SO2276677','SO2276661','SO2276648','SO2276647','SO2276624','SO2276623','SO2276622','SO2276613','SO2276610','SO2276609','SO2276585','SO2276575','SO2276574','SO2276561','SO2276547','SO2276546','SO2276522','SO2276512','SO2276502','SO2276482','SO2276473','SO2276472','SO2276471','SO2276470','SO2276445','SO2276420','SO2276418','SO2276417','SO2276393','SO2276392','SO2276390','SO2276388','SO2276375','SO2276374','SO2276351','SO2276333','SO2276331','SO2276270','SO2276251','SO2276250','SO2276249','SO2276248','SO2276246','SO2276245','SO2276244','SO2276230','SO2276209','SO2276190','SO2276189','SO2276188','SO2276129','SO2276114','SO2276103','SO2276047','SO2276046','SO2276045','SO2276028','SO2276027','SO2276008','SO2275964','SO2275963','SO2275962','SO2275943','SO2275935','SO2275934','SO2275933','SO2275920','SO2275882','SO2275881','SO2275880','SO2275868','SO2275867','SO2275866','SO2275784','SO2275774','SO2275751','SO2275750','SO2275719','SO2275718','SO2275710','SO2275677','SO2275676','SO2275675','SO2275638','SO2275637','SO2275636','SO2275570','SO2275539','SO2275538','SO2275524','SO2275523','SO2275509','SO2275491','SO2275468','SO2275467','SO2275466','SO2275459','SO2275458','SO2275457','SO2275425','SO2275392','SO2275319','SO2275311','SO2275294','SO2275270','SO2275269','SO2275268','SO2275267','SO2275243','SO2275212','SO2275211','SO2275197','SO2275196','SO2275195','SO2275194','SO2275192','SO2275175','SO2275140','SO2275139','SO2275138','SO2275123','SO2275091','SO2275090','SO2275079','SO2275067','SO2275066','SO2275065','SO2275063','SO2275043','SO2275042','SO2275026','SO2275025','SO2274979','SO2274978','SO2274977','SO2274967','SO2274965','SO2273690','SO2274930','SO2274929','SO2274928','SO2274927','SO2274002','SO2274893','SO2274880','SO2274879','SO2274878','SO2274877','SO2274864','SO2274861','SO2274841','SO2274835','SO2274833','SO2274832','SO2274808','SO2274807','SO2274805','SO2274804','SO2274795','SO2274794','SO2274775','SO2274765','SO2274764','SO2274753','SO2274752','SO2274724','SO2274723','SO2274659','SO2274629','SO2272192','SO2274604','SO2274603','SO2274586','SO2274562','SO2274561','SO2274531','SO2274517','SO2274516','SO2274432','SO2274425','SO2274377','SO2274358','SO2274357','SO2274339','SO2274327','SO2274320','SO2274293','SO2274276','SO2274249','SO2274239','SO2274238','SO2274217','SO2274206','SO2274195','SO2274194','SO2274179','SO2274178','SO2274177','SO2274176','SO2274134','SO2274120','SO2274100','SO2274072','SO2274019','SO2274018','SO2274001','SO2273999','SO2273961','SO2273952','SO2273923','SO2273911','SO2273858','SO2273842','SO2273826','SO2273825','SO2273810','SO2273786','SO2273785','SO2273784','SO2273783','SO2273758','SO2273756','SO2273755','SO2273754','SO2273753','SO2273714','SO2273703','SO2273702','SO2273692','SO2273691','SO2273689','SO2273656','SO2273620','SO2273619','SO2273595','SO2273562','SO2273561','SO2273552','SO2273550','SO2273548','SO2273547','SO2273545','SO2273529','SO2273520','SO2273506','SO2273495','SO2273494','SO2273473','SO2273456','SO2273454','SO2273453','SO2273437','SO2273424','SO2273410','SO2273409','SO2273408','SO2273396','SO2273395','SO2273368','SO2273366','SO2273365','SO2273352','SO2273334','SO2273300','SO2273299','SO2273248','SO2273246','SO2273245','SO2273215','SO2273213','SO2273187','SO2273186','SO2273181','SO2273159','SO2273149','SO2273147','SO2273130','SO2273129','SO2273128','SO2273093','SO2273091','SO2273090','SO2273070','SO2273045','SO2273016','SO2272976','SO2272974','SO2272959','SO2272944','SO2272943','SO2272941','SO2272939','SO2272938','SO2272922','SO2272882','SO2272864','SO2272841','SO2272840','SO2272839','SO2272815','SO2272814','SO2272791','SO2272790','SO2272789','SO2272767','SO2272746','SO2272745','SO2272726','SO2272693','SO2272672','SO2272670','SO2272668','SO2272646','SO2272645','SO2272628','SO2272627','SO2272614','SO2272598','SO2272597','SO2272531','SO2272530','SO2272460','SO2272440','SO2272424','SO2272406','SO2272379','SO2272359','SO2272248','SO2272247','SO2272227','SO2272206','SO2272188','SO2271146','SO2272140','SO2272139','SO2272090','SO2272024','SO2272010','SO2271972','SO2271970','SO2271905','SO2271904','SO2271903','SO2271887','SO2271805','SO2271724','SO2271723','SO2271707','SO2271647','SO2271563','SO2271561','SO2270393','SO2271500','SO2271414','SO2271413','SO2271390','SO2271359','SO2271338','SO2271333','SO2271246','SO2271228','SO2271210','SO2271204','SO2271183','SO2271160','SO2271145','SO2271063','SO2271062','SO2271056','SO2271024','SO2270977','SO2270958','SO2270923','SO2270842','SO2270825','SO2270824','SO2270723','SO2270597','SO2270581','SO2270456','SO2270392','SO2270391','SO2270390','SO2270330','SO2270313','SO2270295','SO2270283','SO2270269','SO2270255','SO2270249','SO2270208','SO2270192','SO2270165','SO2270146','SO2270145','SO2270110','SO2270067','SO2270006','SO2269953','SO2269937','SO2269936','SO2269877','SO2269819','SO2269774','SO2269772','SO2269683','SO2269205','SO2269526','SO2269478','SO2269444','SO2269443','SO2269381','SO2269322','SO2269321','SO2269248','SO2269206','SO2269180','SO2269179','SO2269176','SO2269132','SO2269131','SO2269007','SO2268958','SO2268856','SO2268854','SO2268851','SO2268776','SO2268727','SO2268714','SO2268554','SO2268529','SO2268527','SO2268525','SO2268522','SO2268423','SO2268294','SO2268227','SO2268173','SO2268000','SO2267974','SO2267956','SO2267343','SO2266994','SO2266941','SO2266936','SO2266931','SO2266918','SO2266883','SO2266685','SO2266559','SO2266409','SO2266318','SO2266312','SO2266297','SO2266133','SO2266042','SO2265822','SO2265821','SO2265700','SO2265299','SO2265297','SO2265294','SO2265279','SO2265191','SO2264904','SO2264903','SO2264729','SO2264686','SO2264673','SO2264378','SO2264112','SO2264035','SO2263948','SO2263871','SO2263582','SO2263429','SO2263392','SO2262866','SO2257174','SO2256726','SO2256353','SO2255658','SO2253860','SO2253852','SO2253710','SO2087768','SO2064123','SO2062862','SO2062800','SO2062799','SO2233271','SO2233151','SO2233147','SO2233117','SO2233101','SO2233100','SO2233099','SO2233091','SO2233079','SO2233049','SO2233030','SO2233028','SO2232989','SO2232986','SO2232981','SO2232960','SO2232955','SO2232940','SO2232938','SO2232930','SO2232911','SO2232905','SO2232904','SO2232850','SO2232824','SO2232815','SO2232814','SO2232807','SO2232795','SO2232772','SO2232742','SO2232739','SO2232720','SO2232719','SO2232698','SO2232697','SO2232691','SO2232666','SO2232613','SO2232586','SO2232556','SO2232555','SO2232529','SO2232528','SO2232500','SO2232472','SO2232471','SO2232446','SO2232429','SO2232424','SO2232407','SO2232368','SO2232367','SO2232347','SO2232316','SO2232304','SO2232285','SO2232264','SO2232262','SO2232214','SO2232213','SO2232204','SO2232123','SO2232100','SO2232022','SO2232021','SO2231971','SO2230314','SO2231877','SO2231876','SO2231869','SO2231868','SO2231795','SO2229594','SO2231786','SO2231781','SO2231780','SO2231767','SO2231766','SO2231765','SO2231764','SO2231758','SO2231752','SO2229955','SO2231740','SO2231719','SO2231684','SO2231662','SO2231590','SO2231589','SO2231571','SO2231565','SO2231562','SO2231561','SO2231522','SO2231515','SO2231505','SO2231456','SO2231448','SO2231435','SO2231429','SO2231415','SO2231405','SO2231372','SO2231352','SO2231332','SO2231318','SO2231317','SO2231316','SO2231311','SO2231293','SO2231282','SO2231263','SO2231249','SO2231241','SO2231229','SO2231210','SO2231209','SO2231191','SO2231184','SO2231165','SO2231149','SO2231147','SO2231128','SO2231127','SO2231126','SO2231119','SO2231108','SO2231098','SO2231090','SO2231080','SO2231053','SO2231049','SO2231029','SO2231015','SO2231010','SO2230994','SO2230992','SO2230965','SO2230947','SO2230946','SO2230924','SO2230916','SO2230895','SO2230884','SO2230883','SO2230875','SO2230874','SO2230870','SO2230869','SO2230861','SO2230852','SO2230810','SO2230783','SO2230764','SO2230763','SO2230758','SO2230747','SO2230706','SO2230688','SO2230676','SO2230660','SO2230659','SO2230648','SO2230642','SO2230629','SO2230628','SO2230590','SO2230589','SO2230561','SO2230544','SO2230532','SO2230525','SO2230513','SO2230512','SO2230500','SO2230495','SO2230494','SO2230479','SO2230478','SO2230477','SO2230463','SO2230462','SO2230437','SO2230422','SO2230415','SO2230404','SO2230402','SO2230381','SO2230376','SO2230356','SO2230340','SO2230337','SO2230325','SO2230322','SO2230315','SO2230303','SO2230302','SO2230301','SO2230290','SO2230277','SO2230263','SO2230262','SO2230257','SO2230231','SO2230223','SO2230218','SO2230214','SO2230213','SO2230170','SO2230126','SO2230125','SO2230122','SO2230116','SO2230102','SO2230100','SO2230091','SO2230090','SO2230084','SO2230078','SO2230047','SO2230042','SO2230036','SO2230018','SO2230017','SO2230010','SO2230002','SO2229976','SO2229974','SO2229963','SO2229954','SO2229933','SO2229925','SO2229919','SO2229917','SO2229916','SO2229883','SO2229878','SO2229875','SO2229863','SO2229841','SO2229804','SO2229803','SO2229789','SO2229777','SO2229766','SO2229738','SO2229683','SO2229675','SO2229674','SO2229660','SO2229656','SO2229655','SO2229652','SO2229634','SO2229633','SO2229632','SO2229621','SO2229599','SO2229598','SO2229583','SO2229573','SO2229564','SO2229526','SO2229503','SO2229498','SO2229488','SO2229456','SO2229393','SO2229390','SO2229347','SO2229329','SO2229322','SO2229286','SO2229273','SO2229251','SO2229247','SO2229246','SO2229245','SO2229181','SO2229176','SO2229163','SO2229159','SO2229158','SO2229157','SO2229146','SO2229142','SO2229141','SO2229140','SO2229123','SO2229114','SO2229101','SO2229089','SO2229083','SO2229053','SO2229046','SO2229026','SO2229025','SO2229002','SO2229001','SO2228999','SO2228994','SO2228985','SO2228982','SO2228981','SO2228974','SO2228973','SO2228967','SO2228929','SO2228924','SO2228909','SO2228908','SO2228896','SO2228868','SO2228852','SO2228832','SO2228822','SO2228779','SO2228771','SO2228770','SO2228756','SO2228729','SO2228718','SO2228717','SO2228706','SO2228686','SO2228681','SO2228674','SO2228673','SO2228642','SO2228626','SO2228599','SO2228583','SO2228564','SO2228505','SO2228498','SO2228472','SO2228471','SO2228460','SO2228438','SO2228388','SO2228371','SO2228341','SO2228340','SO2228305','SO2228298','SO2228281','SO2228272','SO2228267','SO2228262','SO2228249','SO2228247','SO2228222','SO2228172','SO2228167','SO2228137','SO2228114','SO2228108','SO2228094','SO2228080','SO2228052','SO2228051','SO2228050','SO2228044','SO2228025','SO2228012','SO2227957','SO2227939','SO2227932','SO2227911','SO2227908','SO2227897','SO2227889','SO2227882','SO2227874','SO2227872','SO2227870','SO2227867','SO2227866','SO2227857','SO2227844','SO2227837','SO2227834','SO2227799','SO2227798','SO2227792','SO2227783','SO2227777','SO2227754','SO2227734','SO2227733','SO2227697','SO2227691','SO2227673','SO2227672','SO2227661','SO2227660','SO2227644','SO2227634','SO2227621','SO2227620','SO2227619','SO2227617','SO2227616','SO2227615','SO2227614','SO2227613','SO2227608','SO2227607','SO2227606','SO2227605','SO2227604','SO2227603','SO2227346','SO2227317','SO2226800','SO2226763','SO2226724','SO2226538','SO2226435','SO2226433','SO2226231','SO2226118','SO2225986','SO2225739','SO2224300','SO2349040','SO2349058','SO2349107','SO2349039','SO2349171','SO2349230','SO2349334','SO2349418','SO2349616','SO2349442','SO2349477','SO2349637','SO2349669','SO2349677','SO2349681','SO2361916','SO2359771','SO2350866','SO2349804','SO2350185','SO2349943','SO2350660','SO2349960','SO2349998','SO2350036','SO2350074','SO2350128','SO2350184','SO2350201','SO2350336','SO2350203','SO2350304','SO2350337','SO2350409','SO2350378','SO2350388','SO2350512','SO2350555','SO2350468','SO2350496','SO2350661','SO2350706','SO2350662','SO2351082','SO2350730','SO2350728','SO2350748','SO2362312','SO2350782','SO2350795','SO2360470','SO2350912','SO2353040','SO2350952','SO2351003','SO2351165','SO2351005','SO2351004','SO2351069','SO2351101','SO2351164','SO2351219','SO2351200','SO2351201','SO2351247','SO2351272','SO2351306','SO2351333','SO2351331','SO2351455','SO2351456','SO2353689','SO2354558','SO2351525','SO2352437','SO2351548','SO2351689','SO2351628','SO2351629','SO2351929','SO2354996','SO2366204','SO2351863','SO2351865','SO2351914','SO2352811','SO2351932','SO2352350','SO2352075','SO2352106','SO2352033','SO2352076','SO2352105','SO2352187','SO2354795','SO2352274','SO2352393','SO2364788','SO2354969','SO2352528','SO2352435','SO2361987','SO2352682','SO2352628','SO2352633','SO2352706','SO2352637','SO2356119','SO2352629','SO2352749','SO2352783','SO2353185','SO2354625','SO2352878','SO2352967','SO2352968','SO2353057','SO2353092','SO2353120','SO2353155','SO2353156','SO2354390','SO2360944','SO2355468','SO2353353','SO2354794','SO2353377','SO2353423','SO2354522','SO2353448','SO2353533','SO2353587','SO2353661','SO2353643','SO2353647','SO2353723','SO2353725','SO2353755','SO2353739','SO2353841','SO2353921','SO2353947','SO2353868','SO2356463','SO2354030','SO2360056','SO2357560','SO2354203','SO2354394','SO2354393','SO2354627','SO2354678','SO2354809','SO2355024','SO2354911','SO2354859','SO2354914','SO2354968','SO2355022','SO2355023','SO2357124','SO2356036','SO2357884','SO2355167','SO2355125','SO2355220','SO2355252','SO2355346','SO2355348','SO2355268','SO2355269','SO2355400','SO2355304','SO2358334','SO2355396','SO2355398','SO2357327','SO2355345','SO2355539','SO2355344','SO2357696','SO2355446','SO2355517','SO2355652','SO2355728','SO2355724','SO2364537','SO2356462','SO2355689','SO2355725','SO2355702','SO2357861','SO2355789','SO2355844','SO2355882','SO2356025','SO2356577','SO2356190','SO2356704','SO2356312','SO2361340','SO2356475','SO2356440','SO2356464','SO2356578','SO2356516','SO2356628','SO2356753','SO2356702','SO2356716','SO2356703','SO2356730','SO2356858','SO2356939','SO2356860','SO2356857','SO2356940','SO2356855','SO2357065','SO2357064','SO2357172','SO2357152','SO2357180','SO2359773','SO2357263','SO2357684','SO2357379','SO2357414','SO2357457','SO2357523','SO2357550','SO2357620','SO2357838','SO2357622','SO2357919','SO2360108','SO2357986','SO2358086','SO2357985','SO2367636','SO2358093','SO2359654','SO2358204','SO2363045','SO2358252','SO2358265','SO2361543','SO2358372','SO2358371','SO2359286','SO2358490','SO2358576','SO2363218','SO2358603','SO2358654','SO2360959','SO2358796','SO2358854','SO2358865','SO2358899','SO2358953','SO2365149','SO2359104','SO2359277','SO2359285','SO2359465','SO2359538','SO2359616','SO2360299','SO2359692','SO2359762','SO2359751','SO2359760','SO2359772','SO2359834','SO2360364','SO2361644','SO2361013','SO2359844','SO2359942','SO2359985','SO2360174','SO2360239','SO2360471','SO2360517','SO2360536','SO2360468','SO2360453','SO2363714','SO2360616','SO2361967','SO2360678','SO2361537','SO2360910','SO2360909','SO2362147','SO2361119','SO2361112','SO2361149','SO2361181','SO2361523','SO2363122','SO2361364','SO2370443','SO2361489','SO2361468','SO2361482','SO2364041','SO2361522','SO2361512','SO2361645','SO2361665','SO2364122','SO2361776','SO2361780','SO2363131','SO2364486','SO2364741','SO2361938','SO2362119','SO2361874','SO2364055','SO2362165','SO2362138','SO2362120','SO2362181','SO2368085','SO2362171','SO2363713','SO2362264','SO2362199','SO2362210','SO2362305','SO2362352','SO2362304','SO2362392','SO2362274','SO2362379','SO2362432','SO2368598','SO2362443','SO2362460','SO2362461','SO2362608','SO2362542','SO2363026','SO2362543','SO2362820','SO2362817','SO2363151','SO2363233','SO2363272','SO2363338','SO2363392','SO2363393','SO2363415','SO2363823','SO2363645','SO2363549','SO2364905','SO2363565','SO2363574','SO2363602','SO2363598','SO2363628','SO2363703','SO2363738','SO2363702','SO2363795','SO2364054','SO2363884','SO2363956','SO2363929','SO2365369','SO2363974','SO2363965','SO2364001','SO2364000','SO2364121','SO2364132','SO2364120','SO2364180','SO2364156','SO2364229','SO2364273','SO2364252','SO2364262','SO2364361','SO2364323','SO2364379','SO2364374','SO2364373','SO2364441','SO2364495','SO2364514','SO2364556','SO2364529','SO2365864','SO2364615','SO2364798','SO2365188','SO2364739','SO2364735','SO2364799','SO2364880','SO2364848','SO2364865','SO2364876','SO2364961','SO2364948','SO2365073','SO2365072','SO2365038','SO2365031','SO2365055','SO2365037','SO2365112','SO2365153','SO2365194','SO2365224','SO2365222','SO2365214','SO2365279','SO2365253','SO2365302','SO2365268','SO2365329','SO2365328','SO2365348','SO2365354','SO2365957','SO2365457','SO2365636','SO2367303','SO2365438','SO2365479','SO2369624','SO2365466','SO2365443','SO2365452','SO2365458','SO2365478','SO2365544','SO2365504','SO2365545','SO2365585','SO2365605','SO2365595','SO2365627','SO2365643','SO2365709','SO2365803','SO2365811','SO2365854','SO2365869','SO2366100','SO2365969','SO2365990','SO2366602','SO2366018','SO2366055','SO2366150','SO2366256','SO2366231','SO2366174','SO2366171','SO2366300','SO2366269','SO2366609','SO2366709','SO2366475','SO2366400','SO2366474','SO2368699','SO2366610','SO2366694','SO2366695','SO2366645','SO2366740','SO2366781','SO2367144','SO2366739','SO2366888','SO2366893','SO2366906','SO2367025','SO2367024','SO2367077','SO2367184','SO2367138','SO2367161','SO2367351','SO2367333','SO2367487','SO2367502','SO2367509','SO2367511','SO2368439','SO2367510','SO2367619','SO2367653','SO2367672','SO2368059','SO2367748','SO2367809','SO2368697','SO2368408','SO2367938','SO2367993','SO2368016','SO2368024','SO2368041','SO2368047','SO2368121','SO2368869','SO2368418','SO2368354','SO2368698','SO2368407','SO2368516','SO2368553','SO2368549','SO2368561','SO2368653','SO2368707','SO2368678','SO2368687','SO2368772','SO2368795','SO2368738','SO2368728','SO2368834','SO2368878','SO2368907','SO2368934','SO2368918','SO2368993','SO2369014','SO2369038','SO2369039','SO2369103','SO2369143','SO2369231','SO2369232','SO2369242','SO2369328','SO2369269','SO2369329','SO2369350','SO2370808','SO2369460','SO2371851','SO2369646','SO2371412','SO2369857','SO2369899','SO2369951','SO2370113','SO2370173','SO2370266','SO2370623','SO2370349','SO2370542','SO2371526','SO2370740','SO2370943','SO2370674','SO2370699','SO2370678','SO2370787','SO2370815','SO2370835','SO2370844','SO2370916','SO2370962','SO2370942','SO2371048','SO2370992','SO2371049','SO2371080','SO2371123','SO2371185','SO2371195','SO2371269','SO2371330','SO2371274','SO2371303','SO2371331','SO2371354','SO2371359','SO2371411','SO2371551','SO2371935','SO2371620','SO2371645','SO2371832','SO2371850','SO2371871','SO2371899','SO2371968','SO2372011','SO2372561','SO2333605','SO2358311','SO2332271','SO2326402','SO2327715','SO2342195','SO2325155','SO2336391','SO2340262','SO2369938','SO2326409','SO2366286','SO2336618','SO2325152','SO2355253','SO2342164','SO2361283','SO2362137','SO2324979','SO2348577','SO2355671','SO2364401','SO2364595','SO2326242','SO2346442','SO2354395','SO2335194','SO2359370','SO2351235','SO2334009','SO2334629','SO2354878','SO2333803','SO2330876','SO2356099','SO2330411','SO2348576','SO2360294','SO2353299','SO2357674','SO2337452','SO2331935','SO2370178','SO2365437','SO2371431','SO2349478','SO2333292','SO2347444','SO2324878','SO2332999','SO2336247','SO2336103','SO2346070','SO2342223','SO2320658','SO2321489','SO2367517','SO2344614','SO2328467','SO2321966','SO2365597','SO2367440','SO2369774','SO2349870','SO2330483','SO2361401','SO2336110','SO2336175','SO2339780','SO2333765','SO2342262','SO2325352','SO2365142','SO2355829','SO2324786','SO2326073','SO2327003','SO2343775','SO2324483','SO2361952','SO2362380','SO2326320','SO2367473','SO2347212','SO2371381','SO2336831','SO2317378','SO2317423','SO2349727','SO2315975','SO2339234','SO2353476','SO2319922','SO2318112','SO2318166','SO2371890','SO2327175','SO2324267','SO2347406','SO2319981','SO2329558','SO2320154','SO2319885','SO2319930','SO2320037','SO2320098','SO2320165','SO2320171','SO2320158','SO2320212','SO2320427','SO2322977','SO2327519','SO2320705','SO2320788','SO2326491','SO2321167','SO2321166','SO2325239','SO2321267','SO2321397','SO2321640','SO2365313','SO2321602','SO2324410','SO2321633','SO2345393','SO2331975','SO2322300','SO2322676','SO2324299','SO2330338','SO2322226','SO2322152','SO2322320','SO2330834','SO2322390','SO2324078','SO2322530','SO2333505','SO2322509','SO2324908','SO2322609','SO2322675','SO2322715','SO2323208','SO2330530','SO2327815','SO2322862','SO2322913','SO2323649','SO2322974','SO2323030','SO2324840','SO2323094','SO2325122','SO2346891','SO2326236','SO2323287','SO2324603','SO2324329','SO2323318','SO2333585','SO2323396','SO2325274','SO2323456','SO2323470','SO2323530','SO2323581','SO2323650','SO2323617','SO2323686','SO2323616','SO2323928','SO2323980','SO2324083','SO2324061','SO2324082','SO2324178','SO2324139','SO2325350','SO2324169','SO2324255','SO2324308','SO2324354','SO2324389','SO2329428','SO2324473','SO2324530','SO2324455','SO2325488','SO2324554','SO2324557','SO2325174','SO2324690','SO2324785','SO2344590','SO2324819','SO2324811','SO2324863','SO2324966','SO2324940','SO2324955','SO2325035','SO2325056','SO2325121','SO2325699','SO2325164','SO2325187','SO2325180','SO2325158','SO2325225','SO2325294','SO2325255','SO2326120','SO2325272','SO2325343','SO2325351','SO2325372','SO2342301','SO2325410','SO2325396','SO2325417','SO2325411','SO2325437','SO2325460','SO2338838','SO2325451','SO2325501','SO2325492','SO2325927','SO2325540','SO2336778','SO2325617','SO2327215','SO2332003','SO2325632','SO2325698','SO2325679','SO2325686','SO2341202','SO2349024','SO2325755','SO2325750','SO2326014','SO2325889','SO2326025','SO2325975','SO2325976','SO2325980','SO2326054','SO2326252','SO2360027','SO2326089','SO2326123','SO2326118','SO2337543','SO2326192','SO2333468','SO2343249','SO2326243','SO2326271','SO2326262','SO2326313','SO2327440','SO2326385','SO2326459','SO2326439','SO2327187','SO2326630','SO2326649','SO2326682','SO2326658','SO2326648','SO2326702','SO2326722','SO2326711','SO2326758','SO2349905','SO2327169','SO2326860','SO2326858','SO2343984','SO2326903','SO2326886','SO2326957','SO2326916','SO2331270','SO2327026','SO2327011','SO2327047','SO2327198','SO2327152','SO2335858','SO2327213','SO2327266','SO2327378','SO2327439','SO2327497','SO2327525','SO2327565','SO2327573','SO2327582','SO2327613','SO2327628','SO2327694','SO2327701','SO2327730','SO2328924','SO2327787','SO2327782','SO2327822','SO2327849','SO2327825','SO2327995','SO2327997','SO2330093','SO2328045','SO2328012','SO2355803','SO2328046','SO2328055','SO2328100','SO2328210','SO2328155','SO2329693','SO2328160','SO2328181','SO2328245','SO2328237','SO2328312','SO2360373','SO2328338','SO2328336','SO2328372','SO2328376','SO2329099','SO2329504','SO2328528','SO2328638','SO2336777','SO2328554','SO2328560','SO2328571','SO2328686','SO2328727','SO2328958','SO2328728','SO2328735','SO2328738','SO2328741','SO2328791','SO2328780','SO2352723','SO2328829','SO2328833','SO2328922','SO2328950','SO2328957','SO2328974','SO2329070','SO2330276','SO2329187','SO2329223','SO2329239','SO2329352','SO2329343','SO2329371','SO2329397','SO2335804','SO2329455','SO2329476','SO2329530','SO2329559','SO2329538','SO2330701','SO2329505','SO2329605','SO2329621','SO2329670','SO2333918','SO2329687','SO2329813','SO2329760','SO2329931','SO2329837','SO2329946','SO2331644','SO2330051','SO2344198','SO2330964','SO2330037','SO2341081','SO2330061','SO2330684','SO2330190','SO2330293','SO2326319']

actual_batch = []
batch_count = 0

for item in so_names:
    #print(f"Se procesa la orden: {item}")
    actual_batch.append(item)

    if len(actual_batch) >= 1000:
        print(f"Numero de items en el batch {batch_count + 1}: {len(actual_batch)}")
        batch_count += 1
        actual_batch = []

if len(actual_batch) > 0:
    print(f"Numero de items en el ultimo batch: {len(actual_batch)}")