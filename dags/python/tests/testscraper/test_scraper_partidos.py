import pytest
from bs4 import BeautifulSoup as bs4
import pandas as pd
import time
from datetime import datetime, timedelta

from src.scraper_partidos import ScraperPartidos
from src.excepciones import ErrorFechaFormato, ErrorFechaPosterior, PaginaError, PartidosExtraidosError

@pytest.mark.parametrize(["fecha"],
	[("201906-22",), ("22/06/2019",), ("22062019",), ("2019-0622",), ("2019-06/22",)]
)
def test_crear_objeto_scraper_partidos_error_formato(fecha):

	with pytest.raises(ErrorFechaFormato):

		ScraperPartidos(fecha)

@pytest.mark.parametrize(["dias"],
	[(1,), (10,), (100,), (1000,), (10000,)]
)
def test_crear_objeto_scraper_partidos_error_fecha_posterior(dias):

	fecha=(datetime.now() + timedelta(days=dias)).strftime("%Y-%m-%d")

	with pytest.raises(ErrorFechaPosterior):

		ScraperPartidos(fecha)

@pytest.mark.parametrize(["fecha"],
	[("2019-06-22",), ("2019-04-13",), (datetime.now().strftime("%Y-%m-%d"),)]
)
def test_crear_objeto_scraper_partidos(fecha):

	scraper_partidos=ScraperPartidos(fecha)

	assert scraper_partidos.obtenerFecha()==fecha

def test_scraper_partidos_realizar_peticion(scraper_partidos):

	contenido=scraper_partidos._Scraper__realizarPeticion()

	assert isinstance(contenido, bs4)

def test_scraper_partidos_contenido_a_tablas(scraper_partidos):

	contenido=scraper_partidos._Scraper__realizarPeticion()

	tablas=scraper_partidos._ScraperPartidos__contenido_a_tablas(contenido)

	assert isinstance(tablas, list)

def test_scraper_partidos_titulo(scraper_partidos):

	contenido=scraper_partidos._Scraper__realizarPeticion()

	tablas=scraper_partidos._ScraperPartidos__contenido_a_tablas(contenido)

	tabla=tablas[0]

	titulo=scraper_partidos._ScraperPartidos__obtenerTitulo(tabla)

	assert isinstance(titulo, str)

def test_scraper_partidos_titulo_tabla(scraper_partidos):

	contenido=scraper_partidos._Scraper__realizarPeticion()

	tablas=scraper_partidos._ScraperPartidos__contenido_a_tablas(contenido)

	tabla=tablas[0]

	titulo_tabla=scraper_partidos._ScraperPartidos__obtenerTituloTabla(tabla)

	assert isinstance(titulo_tabla, str)

def test_scraper_partidos_ambos_titulos(scraper_partidos):

	contenido=scraper_partidos._Scraper__realizarPeticion()

	tablas=scraper_partidos._ScraperPartidos__contenido_a_tablas(contenido)

	tabla=tablas[0]

	titulo=scraper_partidos._ScraperPartidos__obtenerTitulo(tabla)

	titulo_tabla=scraper_partidos._ScraperPartidos__obtenerTituloTabla(tabla)

	assert titulo==titulo_tabla

def test_scraper_partidos_columnas(scraper_partidos):

	contenido=scraper_partidos._Scraper__realizarPeticion()

	tablas=scraper_partidos._ScraperPartidos__contenido_a_tablas(contenido)

	tabla=tablas[0]

	columnas=scraper_partidos._ScraperPartidos__obtenerColumnas(tabla)

	assert isinstance(columnas, list)
	assert len(columnas)==13

def test_scraper_partidos_contenido_filas(scraper_partidos):

	contenido=scraper_partidos._Scraper__realizarPeticion()

	tablas=scraper_partidos._ScraperPartidos__contenido_a_tablas(contenido)

	tabla=tablas[0]

	contenido_filas=scraper_partidos._ScraperPartidos__obtenerContenidoFilas(tabla)

	assert isinstance(contenido_filas, list)

	for fila in contenido_filas:

		assert isinstance(fila, list)

def test_scraper_partidos_limpiar_tabla(scraper_partidos):

	contenido=scraper_partidos._Scraper__realizarPeticion()

	tablas=scraper_partidos._ScraperPartidos__contenido_a_tablas(contenido)

	tabla=tablas[0]

	tabla_limpia=scraper_partidos._ScraperPartidos__limpiarTabla(tabla)

	assert isinstance(tabla_limpia, pd.DataFrame)

def test_scraper_partidos_obtener_data_limpia(scraper_partidos):

	contenido=scraper_partidos._Scraper__realizarPeticion()

	tablas=scraper_partidos._ScraperPartidos__contenido_a_tablas(contenido)

	tablas_limpias=scraper_partidos._ScraperPartidos__obtenerDataLimpia(tablas)

	assert isinstance(tablas_limpias, pd.DataFrame)
	assert not tablas_limpias.empty

def test_scraper_partidos_obtener_data_limpia_error():

	scraper_partidos=ScraperPartidos("1800-01-01")

	contenido=scraper_partidos._Scraper__realizarPeticion()

	tablas=scraper_partidos._ScraperPartidos__contenido_a_tablas(contenido)

	with pytest.raises(PartidosExtraidosError):

		scraper_partidos._ScraperPartidos__obtenerDataLimpia(tablas)

def test_scraper_partidos_obtener_partidos_error():

	scraper_partidos=ScraperPartidos("1800-01-01")

	with pytest.raises(PartidosExtraidosError):

		scraper_partidos.obtenerPartidos()

def test_scraper_partidos_obtener_partidos(scraper_partidos):

	data=scraper_partidos.obtenerPartidos()

	assert isinstance(data, pd.DataFrame)
	assert not data.empty

	print(data)

	time.sleep(60)