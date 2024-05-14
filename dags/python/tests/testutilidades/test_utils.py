import pytest
import os
import time
from datetime import datetime, timedelta

from src.utils import descargar, realizarDescarga, entorno_creado, crearEntornoDataLake, subirArchivosDataLake
from src.utils import obtenerFechaInicio, generarFechas, fechas_etl, etl_partidos_disponibles
from src.excepciones import DescargaImagenError

@pytest.mark.parametrize(["nueva", "antigua"],
	[
		(None, "url_antigua"),
		(None, None),
		("url_antigua", "url_antigua"),
		("url_nueva", "url_nueva")
	]
)
def test_descargar_no(nueva, antigua):

	assert descargar(nueva, antigua) is False

@pytest.mark.parametrize(["nueva", "antigua"],
	[
		("url_antigua", None),
		("url_nueva", "url_antigua")
	]
)
def test_descargar_si(nueva, antigua):

	assert descargar(nueva, antigua)

@pytest.mark.parametrize(["url"],
	[(None,), ("url_antigua",), ("url_nueva",)]
)
def test_realizar_descarga_error(url):

	with pytest.raises(DescargaImagenError):

		realizarDescarga(url, "ruta", "nombre")

def borrarCarpeta(ruta:str)->None:

	if os.path.exists(ruta):

		os.rmdir(ruta)

def crearCarpeta(ruta:str)->None:

	if not os.path.exists(ruta):

		os.mkdir(ruta)

def vaciarCarpeta(ruta:str)->None:

	if os.path.exists(ruta):

		for archivo in os.listdir(ruta):

			os.remove(os.path.join(ruta, archivo))

def test_realizar_descarga():

	ruta_carpeta=os.path.join(os.getcwd(), "testutilidades", "Imagenes_Tests")

	crearCarpeta(ruta_carpeta)

	vaciarCarpeta(ruta_carpeta)

	url_imagen="https://cdn.ssref.net/req/202404172/tlogo/fb/db3b9613.png"

	realizarDescarga(url_imagen, ruta_carpeta, "atleti")

	assert os.path.exists(os.path.join(ruta_carpeta, "atleti.png"))

	vaciarCarpeta(ruta_carpeta)

	borrarCarpeta(ruta_carpeta)

def test_entorno_creado_no_creado():

	assert not entorno_creado("contenedor3")

def test_entorno_creado(datalake):

	datalake.crearContenedor("contenedor3")

	time.sleep(2)

	assert entorno_creado("contenedor3")

	datalake.eliminarContenedor("contenedor3")

	datalake.cerrarConexion()

def test_crear_entorno_data_lake(datalake):

	crearEntornoDataLake("contenedor4", "carpeta4")

	time.sleep(2)

	assert entorno_creado("contenedor4")

	datalake.cerrarConexion()

def test_subir_archivo_data_lake_contenedor_no_existe():

	with pytest.raises(Exception):

		subirArchivosDataLake("contenedornacho", "carpeta", "ruta_local")

def test_subir_archivo_data_lake_carpeta_no_existe():

	with pytest.raises(Exception):

		subirArchivosDataLake("contenedor4", "carpeta", "ruta_local")

def test_subir_archivo_data_lake_local_no_existe(datalake):

	datalake.crearCarpeta("contenedor4", "carpeta_creada")

	with pytest.raises(Exception):

		subirArchivosDataLake("contenedor4", "carpeta_creada", "ruta_local")

	datalake.cerrarConexion()

def test_subir_archivo_data_lake_archivo_no_existen(datalake):

	ruta_carpeta=os.path.join(os.getcwd(), "Archivos_Tests_Data_Lake")

	crearCarpeta(ruta_carpeta)

	subirArchivosDataLake("contenedor4", "carpeta_creada", ruta_carpeta)

	archivos_carpeta_contenedor=datalake.paths_carpeta_contenedor("contenedor4", "carpeta_creada")

	assert not archivos_carpeta_contenedor

	datalake.cerrarConexion()

def crearArchivoTXT(ruta:str, nombre:str)->None:

	ruta_archivo=os.path.join(ruta, nombre)

	with open(ruta_archivo, "w") as file:

	    file.write("Nacho")

def test_subir_archivo_data_lake(datalake):

	ruta_carpeta=os.path.join(os.getcwd(), "Archivos_Tests_Data_Lake")

	nombre_archivo="archivo.txt"

	crearArchivoTXT(ruta_carpeta, nombre_archivo)

	subirArchivosDataLake("contenedor4", "carpeta_creada", ruta_carpeta)

	archivos_carpeta_contenedor_nuevos=datalake.paths_carpeta_contenedor("contenedor4", "carpeta_creada")

	assert len(archivos_carpeta_contenedor_nuevos)==1

	datalake.eliminarContenedor("contenedor4")

	datalake.cerrarConexion()

	vaciarCarpeta(ruta_carpeta)

def test_subir_archivo_data_lake_archivo_existente(datalake):

	crearEntornoDataLake("contenedor5", "carpeta")

	ruta_carpeta=os.path.join(os.getcwd(), "Archivos_Tests_Data_Lake")

	nombre_archivo="archivo.txt"

	crearArchivoTXT(ruta_carpeta, nombre_archivo)

	datalake.subirArchivo("contenedor5", "carpeta", ruta_carpeta, nombre_archivo)

	archivos_carpeta_contenedor=datalake.paths_carpeta_contenedor("contenedor5", "carpeta")

	assert len(archivos_carpeta_contenedor)==1

	subirArchivosDataLake("contenedor5", "carpeta", ruta_carpeta)

	archivos_carpeta_contenedor_nuevos=datalake.paths_carpeta_contenedor("contenedor5", "carpeta")

	assert len(archivos_carpeta_contenedor_nuevos)==1

	datalake.eliminarContenedor("contenedor5")

	datalake.cerrarConexion()

	vaciarCarpeta(ruta_carpeta)

def test_subir_archivo_data_lake_archivos_existentes_no_existentes(datalake):

	crearEntornoDataLake("contenedor6", "carpeta")

	ruta_carpeta=os.path.join(os.getcwd(), "Archivos_Tests_Data_Lake")

	nombre_archivos_subir=[f"archivo{numero}_subir.txt" for numero in range(1,6)]

	for nombre_archivo in nombre_archivos_subir:

		crearArchivoTXT(ruta_carpeta, nombre_archivo)

		datalake.subirArchivo("contenedor6", "carpeta", ruta_carpeta, nombre_archivo)

	nombre_archivos_no_subir=[f"archivo{numero}_no_subir.txt" for numero in range(1,6)]

	for nombre_archivo in nombre_archivos_no_subir:

		crearArchivoTXT(ruta_carpeta, nombre_archivo)

	archivos_carpeta_contenedor=datalake.paths_carpeta_contenedor("contenedor6", "carpeta")

	assert len(archivos_carpeta_contenedor)==5
	assert len(os.listdir(ruta_carpeta))==10

	subirArchivosDataLake("contenedor6", "carpeta", ruta_carpeta)

	archivos_carpeta_contenedor_nuevos=datalake.paths_carpeta_contenedor("contenedor6", "carpeta")

	assert len(archivos_carpeta_contenedor_nuevos)==10

	datalake.eliminarContenedor("contenedor6")

	datalake.cerrarConexion()

	vaciarCarpeta(ruta_carpeta)

	borrarCarpeta(ruta_carpeta)

@pytest.mark.parametrize(["fecha_inicio"],
	[("2024-01-01",), ("2024-05-10",), ("2010-06-22",),("1998-02-16",)]
)
def test_obtener_fecha_inicio_tabla_vacia(conexion, fecha_inicio):

	assert obtenerFechaInicio(fecha_inicio)==fecha_inicio

def test_obtener_fecha_inicio_tabla_registro(conexion):

	partido=["Champions", "Final", "2019-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"]

	conexion.insertarPartido(partido)

	assert obtenerFechaInicio("2019-04-13")=="2019-06-23"

def test_obtener_fecha_inicio_tabla_registros(conexion):

	partidos=[["Champions", "Final", "2019-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2020-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2019-07-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2023-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2024-04-13","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2022-06-13","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"]]

	conexion.insertarPartidos(partidos)

	assert obtenerFechaInicio("2024-01-01")=="2024-04-14"

@pytest.mark.parametrize(["inicio", "fin"],
	[
		("2019-06-22", "2019-04-13"),
		("2023-06-22", "2019-04-13"),
		("2019-06-22", "2019-06-21"),
		("2019-07-22", "2019-04-13"),
		("2019-06-22", "2019-06-13")
	]
)
def test_generar_fechas_inicio_superior(inicio, fin):

	assert not generarFechas(inicio, fin)

def test_generar_fechas_inicio_fin_iguales():

	assert len(generarFechas("2019-06-22", "2019-06-22"))==1

def test_fechas_etl_tabla_vacia(conexion):

	fechas=fechas_etl()

	assert fechas[0]=="2024-01-01"
	assert fechas[-1]==(datetime.now().date()-timedelta(days=3)).strftime("%Y-%m-%d")

def test_fechas_etl_tabla_registro(conexion):

	partido=["Champions", "Final", "2024-01-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"]

	conexion.insertarPartido(partido)

	fechas=fechas_etl()

	assert fechas[0]=="2024-01-23"
	assert fechas[-1]==(datetime.now().date()-timedelta(days=3)).strftime("%Y-%m-%d")

def test_fechas_etl_tabla_registros(conexion):

	partidos=[["Champions", "Final", "2019-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2020-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2019-07-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2023-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2024-01-21","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2022-06-13","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"]]

	conexion.insertarPartidos(partidos)

	fechas=fechas_etl()

	assert fechas[0]=="2024-01-22"
	assert fechas[-1]==(datetime.now().date()-timedelta(days=3)).strftime("%Y-%m-%d")

def test_fechas_etl_tabla_dia_anterior(conexion):

	dia_anterior=(datetime.now().date()-timedelta(days=4)).strftime("%Y-%m-%d")
	dia_ultimo=(datetime.now().date()-timedelta(days=3)).strftime("%Y-%m-%d")

	partidos=[["Champions", "Final", "2019-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2020-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2019-07-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2023-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", dia_anterior,"21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2022-06-13","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"]]

	conexion.insertarPartidos(partidos)

	fechas=fechas_etl()

	assert fechas[0]==dia_ultimo
	assert fechas[-1]==dia_ultimo
	assert len(fechas)==1

def test_fechas_etl_tabla_dia_ultimo(conexion):

	dia_ultimo=(datetime.now().date()-timedelta(days=3)).strftime("%Y-%m-%d")

	partidos=[["Champions", "Final", "2019-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2020-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2019-07-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2023-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", dia_ultimo,"21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2022-06-13","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"]]

	conexion.insertarPartidos(partidos)

	assert not fechas_etl()

def test_etl_partidos_disponibles_no_ligas_no_equipos(conexion):

	assert not etl_partidos_disponibles()

def test_etl_partidos_disponibles_no_equipos(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	assert not etl_partidos_disponibles()

def test_etl_partidos_disponibles(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", "url12345codigo", "Atleti", id_liga]

	conexion.insertarEquipo(equipo)

	assert etl_partidos_disponibles()