import pandas as pd

# ***********REVISAR QUE NO DEVUELVA EL VACIO EN Lorder_list ********
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

    return  orders_list, placeholders, num_records

if __name__ == "__main__":
    # Example usage
    file_path = 'C:/Users/Sergio Gil Guerrero/Documents/WonderBrands/Finanzas/Marzo/Notas_de_credito_totales_ML_test.csv'  # Replace with the correct file path
    type_filter = 'INDIVIDUAL'  # Can be 'INDIVIDUAL' or 'GLOBAL'
    marketplace_filter = 'MERCADO LIBRE'  # Replace with the desired marketplace

    names_concatenated, placeholders, num_records = filter_orders(file_path, type_filter, marketplace_filter)
    print(num_records, names_concatenated, placeholders)
