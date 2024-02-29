import os
import shutil
from concurrent.futures import ThreadPoolExecutor
import time
from tqdm import tqdm

def copy_file(file_path, destination_directory, pbar):
    file_name = os.path.basename(file_path)
    shutil.copy(file_path, os.path.join(destination_directory, file_name))
    #if pbar != True:
    pbar.update(1)  # Actualizamos la barra de progreso cada vez que se copia un archivo
    #print(f"Archivo '{file_name}' copiado a la carpeta raíz.")

def copy_files_into_root_parallel(root_directory):
    files_to_copy = []
    # Obtener todas las rutas completas de los archivos en las subcarpetas
    for root_path, _, files in os.walk(root_directory):
        for file in files:
            if file != 'desktop.ini':  # Verificar si el archivo no es desktop.ini
                full_file_path = os.path.join(root_path, file)
                files_to_copy.append(full_file_path)

    # Crear una única barra de progreso para el total de archivos
    with tqdm(total=len(files_to_copy), desc="Extracting files", unit="file") as pbar:
        with ThreadPoolExecutor() as executor:
            for file in files_to_copy:
                executor.submit(copy_file, file, root_directory, pbar)
def copy_files_into_root(root_directory):
    # Obtener todas las rutas completas de los archivos en las subcarpetas
    files_to_copy = []
    for root_path, directories, files in os.walk(root_directory):
        for file in files:
            if file != 'desktop.ini':  # Verificar si el archivo no es desktop.ini
                # Ruta completa del archivo
                full_file_path = os.path.join(root_path, file)
                files_to_copy.append(full_file_path)

    # Crear una única barra de progreso para el total de archivos
    with tqdm(total=len(files_to_copy), desc="Extracting files", unit="file") as pbar:
                for file in files_to_copy:
                    copy_file(file, root_directory, pbar)

def remove_files(directory):
    # Iterar sobre todos los elementos en la carpeta
    for element in os.listdir(directory):
        element_path = os.path.join(directory, element)
        # Verificar si es un archivo
        if os.path.isfile(element_path):
            # Eliminar el archivo
            os.remove(element_path)
            print(f"File '{element}' removed.")
        # Si es una carpeta, no hacer nada
        elif os.path.isdir(element_path):
            continue

if __name__ == "__main__":

    # Carpeta raíz (ruta completa)
    root_directory = r'G:\.shortcut-targets-by-id\1vsZk0-0Cd1FnEKNQlXzq3EuSgg6ZRgtP\2024\202402'

    # Llamar a la función para mover los archivos de las subcarpetas a la carpeta raíz
    start = time.time()
    copy_files_into_root_parallel(root_directory) #avg = 78 files/seg
    #copy_files_into_root(root_directory) #avg = 45 files/seg
    end = time.time()
    print(f"Tiempo de ejecución: {end-start} segundos")

    # Ruta de la carpeta a limpiar
    folder_to_clean = r'G:\.shortcut-targets-by-id\1vsZk0-0Cd1FnEKNQlXzq3EuSgg6ZRgtP\2024\202402'
    #remove_files(folder_to_clean)

