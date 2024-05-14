import wget
import os
from datetime import datetime, timedelta
from typing import List, Optional

from .excepciones import DescargaImagenError
from .datalake.conexion_data_lake import ConexionDataLake
from .database.conexion import Conexion

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

def obtenerFechaInicio(fecha_inicio:str)->str:

	con=Conexion()

	if con.tabla_vacia():

		inicio=fecha_inicio

	else:

		ultima_fecha=con.fecha_maxima()

		ultima_fecha_datetime=datetime.strptime(ultima_fecha, "%Y-%m-%d")

		inicio=(ultima_fecha_datetime+timedelta(days=1)).strftime("%Y-%m-%d")

	con.cerrarConexion()

	return inicio

def generarFechas(inicio:str, fin:str)->List[Optional[str]]:

	inicio_datetime=datetime.strptime(inicio, "%Y-%m-%d")

	fin_datetime=datetime.strptime(fin, "%Y-%m-%d")

	fechas=[]

	while inicio_datetime<=fin_datetime:

		fechas.append(inicio_datetime.strftime("%Y-%m-%d"))

		inicio_datetime+=timedelta(days=1)

	return fechas

def fechas_etl(fecha_inicio:str="2024-01-01")->List[Optional[str]]:

	inicio=obtenerFechaInicio(fecha_inicio)

	fin=(datetime.now().date()-timedelta(days=3)).strftime("%Y-%m-%d")

	return generarFechas(inicio, fin)

def etl_partidos_disponibles()->bool:

	con=Conexion()

	if con.tabla_vacia("ligas") or con.tabla_vacia("equipos"):

		con.cerrarConexion()

		return False

	con.cerrarConexion()

	return True