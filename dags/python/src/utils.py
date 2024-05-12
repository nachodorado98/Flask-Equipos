import wget
import os

from .excepciones import DescargaImagenError
from .datalake.conexion_data_lake import ConexionDataLake

# Funcion para verificar si hay que descargar o no la imagen
def descargar(url_nueva:str, url_antigua:str)->bool:

	return False if url_nueva is None or url_nueva==url_antigua else True

# Funcion para realizar la descarga de la imagen
def realizarDescarga(url_imagen:str, ruta_imagenes:str, nombre:str)->None:

	try:
		
		wget.download(url_imagen, os.path.join(ruta_imagenes, f"{nombre}.png"))
	
	except Exception as e:
	
		raise DescargaImagenError(f"No se ha podido descargar la imagen de {nombre}")

def entorno_creado(nombre_contenedor:str)->bool:

	datalake=ConexionDataLake()

	contenedores=datalake.contenedores_data_lake()

	datalake.cerrarConexion()

	contenedor_existe=[contenedor for contenedor in contenedores if nombre_contenedor in contenedor["name"]]

	return False if not contenedor_existe else True

def crearEntornoDataLake(nombre_contenedor:str, nombre_carpeta:str)->None:

	datalake=ConexionDataLake()

	datalake.crearContenedor(nombre_contenedor)

	datalake.crearCarpeta(nombre_contenedor, nombre_carpeta)

	datalake.cerrarConexion()

def subirArchivosDataLake(nombre_contenedor:str, nombre_carpeta:str, ruta_local_carpeta:str)->None:

	datalake=ConexionDataLake()

	archivos_carpeta_contenedor=datalake.paths_carpeta_contenedor(nombre_contenedor, nombre_carpeta)

	archivos_carpeta_contenedor_limpios=[archivo.name.split(f"{nombre_carpeta}/")[1] for archivo in archivos_carpeta_contenedor]

	try:

		archivos_local=os.listdir(ruta_local_carpeta)

	except Exception:

		raise Exception("La ruta de la carpeta local de los archivos es incorrecta")

	for archivo_local in archivos_local:

		if archivo_local not in archivos_carpeta_contenedor_limpios:

			datalake.subirArchivo(nombre_contenedor, nombre_carpeta, ruta_local_carpeta, archivo_local)