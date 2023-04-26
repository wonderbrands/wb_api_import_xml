Odoo External API

Este módulo contiene funciones para consulta e inserción de información al sistema Odoo v15

-Actualizaciones de cantidades entregadas dentro de la orden de venta

    -El programa actualiza la columna qty_delivered con la cantidad de la columna prouct_uom_qty en sale_order_line

-Creación automática de facturas a partir de una SO específica

    -Facturas creadas por SO
        El programa crea una factura con todo el contenido de la orden de venta
    -Facturas creadas por número de items
        El programa crea una factura por cada item en qty_delivered para cada SKU
    -Facturas creadas por SKU
        El programa crea una factura por cada SKU en la orden de venta

-Inserción de archivos dentro del modelo ir.attachment 
    
    -Adjunta de manera automática archivos dentro de la sección ir.attachment