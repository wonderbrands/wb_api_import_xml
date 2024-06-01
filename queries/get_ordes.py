import pandas as pd
import mysql.connector
from openpyxl import Workbook
import json

config_file_name = r'C:\Users\Sergio Gil Guerrero\Documents\WonderBrands\Repos\wb_odoo_external_api\config\config_dev.json'

def get_psql_access():
    with open(config_file_name, 'r') as config_file:
        config = json.load(config_file)


def execute_query_to_excel(sql_query, params, output_file):
    # Conexión a la base de datos
    mydb = mysql.connector.connect(
        host="tu_host",
        user="tu_usuario",
        password="tu_contraseña",
        database="tu_base_de_datos"
    )
    mycursor = mydb.cursor()

    # Ejecutar la consulta SQL
    mycursor.execute(sql_query, params)
    results = mycursor.fetchall()

    # Convertir los resultados a un DataFrame de pandas
    df = pd.DataFrame(results, columns=mycursor.column_names)

    # Guardar los resultados en un archivo Excel
    with pd.ExcelWriter(output_file) as writer:
        df.to_excel(writer, sheet_name='Resultados', index=False)

# Definir las fechas de inicio y fin
start_date_str = '2024-01-01'
end_date_str = '2024-01-20'

# Consulta SQL para INDIVIDUALES
query_individual = """
SELECT c.name,
       d.order_id 'order_id',
       b.amount_total 'total_factura',
       b.amount_untaxed 'subtotal_factura',
       d.refunded_amt,
       d.refund_date,
       b.invoice_partner_display_name 'cliente',
       b.name,
       b.id 'account_move_id',
       'INDIVIDUAL' as type,
       'AMAZON' as marketplace
FROM somos_reyes.odoo_new_account_move_aux b
LEFT JOIN somos_reyes.odoo_new_sale_order c
ON b.invoice_origin = c.name
LEFT JOIN (SELECT a.order_id, max(STR_TO_DATE(fecha, '%d/%m/%Y')) 'refund_date', SUM(total - tarifas_de_amazon) * (-1) 'refunded_amt'
           FROM somos_reyes.amazon_payments_refunds a
           WHERE (total - tarifas_de_amazon) * (-1) > 0 AND STR_TO_DATE(fecha, '%d/%m/%Y') >= %s AND STR_TO_DATE(fecha, '%d/%m/%Y') <= %s
           GROUP BY 1) d
ON c.channel_order_id = d.order_id
LEFT JOIN (SELECT distinct invoice_origin FROM somos_reyes.odoo_new_account_move_aux WHERE name like '%RINV%') e
ON c.name = e.invoice_origin
WHERE d.order_id is not null
AND e.invoice_origin is null
AND d.refunded_amt - b.amount_total < 1 AND d.refunded_amt - b.amount_total > -1;
"""

# Llamar a la función para ejecutar la consulta SQL y guardar los resultados en un archivo Excel
execute_query_to_excel(query_individual, (start_date_str, end_date_str), 'resultados_individuales.xlsx')
