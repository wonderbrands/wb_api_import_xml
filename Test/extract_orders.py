import pandas as pd

def filter_orders(csv_file, type_val, marketplace_val):
    df = pd.read_csv(csv_file)

    # Filter by type and marketplace
    df_filtered = df[(df['type'] == type_val) & (df['marketplace'] == marketplace_val)]

    # Get the list of names
    orders_list = df_filtered['name'].tolist()

    # Format names list for SQL query
    placeholders = ','.join(['%s'] * len(orders_list))

    # Get concatenated names
    #concatenated_names = ','.join(f"'{name}'" for name in df_filtered['name'])

    # Get the number of matching records
    num_records = len(df_filtered)

    # REVISA QUE NO DEVUELVA EL VACIO EN Lorder_list
    if num_records == 0:
        return [''],'%s',0
    else:
        return orders_list, placeholders, num_records

def marketplace_references(csv_file):
    df = pd.read_csv(csv_file)

    # Get the list of marketplace references
    marketplace_refs_list = df['Marketplace Reference'].tolist()

    formatted_marketplace_refs = [str(ref) for ref in marketplace_refs_list]

    # Format marketplace references list for SQL query
    placeholders = ','.join(['%s'] * len(marketplace_refs_list))

    # Get the number of marketplace references
    num_marketplace_refs = len(marketplace_refs_list)

    if num_marketplace_refs == 0:
        return [''],'%s',0
    else:
        return formatted_marketplace_refs, placeholders, num_marketplace_refs

    return marketplace_refs_list, placeholders, num_marketplace_refs

def split_csv_to_excel(csv_file, chunk_size, output_dir):
    # Cargar el archivo CSV en un DataFrame
    df = pd.read_csv(csv_file)

    # Calcular el número de filas y el número de fragmentos
    total_rows = len(df)
    num_chunks = total_rows // chunk_size
    last_chunk_size = total_rows % chunk_size

    # Añadir un fragmento extra si hay residuo
    if last_chunk_size > 0:
        num_chunks += 1

    # Dividir el DataFrame en fragmentos y guardar cada fragmento como un archivo Excel
    for chunk_num in range(num_chunks):
        start_idx = chunk_num * chunk_size
        end_idx = min((chunk_num + 1) * chunk_size, total_rows)
        chunk_df = df.iloc[start_idx:end_idx]

        # Asignar el nombre de la columna
        chunk_df.columns = ['so_name']  # Nombre de la columna en el archivo de salida

        # Crear un nuevo nombre de archivo para el fragmento
        output_file = "{}/so_invoices{}.xlsx".format(output_dir, chunk_num + 1)

        # Guardar el fragmento en un archivo Excel con el nombre de la columna modificado
        chunk_df.to_excel(output_file, index=False)

    print(f"Se han creado {num_chunks} archivos Excel en {output_dir}")
    return num_chunks


if __name__ == "__main__":
    # # Example usage
    # file_path = 'C:/Users/Sergio Gil Guerrero/Documents/WonderBrands/Finanzas/Marzo/Notas_de_credito_totales_ML_test.csv'
    # type_filter = 'INDIVIDUAL'  # Can be 'INDIVIDUAL' or 'GLOBAL'
    # marketplace_filter = 'MERCADO LIBRE'  # Replace with the desired marketplace
    #
    # names_concatenated, placeholders, num_records = filter_orders(file_path, type_filter, marketplace_filter)
    # print(num_records, names_concatenated, placeholders)
    #
    # file_path_mk = 'C:/Users/Sergio Gil Guerrero/Documents/WonderBrands/Finanzas/Marzo/Walmart/autofacturacion.csv'
    #
    # marketplace_refs, placeholders, num_marketplace_refs = marketplace_references(file_path_mk)
    # print(num_marketplace_refs, marketplace_refs, placeholders)

    excel_files_dir = 'C:/Users/Sergio Gil Guerrero/Documents/WonderBrands/Repos/wb_odoo_external_api/invoices_functions/files/invoices_test'
    file_path_walmart = 'C:/Users/Sergio Gil Guerrero/Documents/WonderBrands/Finanzas/Marzo/Walmart/facturacion_global.csv'
    num_of_runs = split_csv_to_excel(file_path_walmart,999,excel_files_dir)

