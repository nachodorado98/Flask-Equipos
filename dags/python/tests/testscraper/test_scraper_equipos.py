import pytest
from bs4 import BeautifulSoup as bs4
import pandas as pd
import time

from src.scraper_equipos import ScraperEquipos
from src.excepciones import PaginaError, EquiposError

def test_crear_objeto_scraper_equipos():

	scraper=ScraperEquipos("endpoint")

def test_scraper_equipos_realizar_peticion_error(scraper):

	scraper=ScraperEquipos("/endpoint")

	with pytest.raises(PaginaError):

		scraper._Scraper__realizarPeticion()

def test_scraper_equipos_realizar_peticion(scraper_equipos):

	contenido=scraper_equipos._Scraper__realizarPeticion()

	assert isinstance(contenido, bs4)

def test_scraper_equipos_obtener_tabla(scraper_equipos):

	contenido=scraper_equipos._Scraper__realizarPeticion()

	tabla=scraper_equipos._ScraperEquipos__contenido_a_tabla(contenido)

	assert tabla is not None

def test_scraper_equipos_obtener_columnas(scraper_equipos):

	contenido=scraper_equipos._Scraper__realizarPeticion()

	tabla=scraper_equipos._ScraperEquipos__contenido_a_tabla(contenido)

	columnas=scraper_equipos._ScraperEquipos__obtenerColumnas(tabla)

	assert isinstance(columnas, list)
	assert len(columnas)==9

def test_scraper_equipos_obtener_filas(scraper_equipos):

	contenido=scraper_equipos._Scraper__realizarPeticion()

	tabla=scraper_equipos._ScraperEquipos__contenido_a_tabla(contenido)

	filas=scraper_equipos._ScraperEquipos__obtenerFilas(tabla)

	assert isinstance(filas, list)

def test_scraper_equipos_obtener_contenido_filas(scraper_equipos):

	contenido=scraper_equipos._Scraper__realizarPeticion()

	tabla=scraper_equipos._ScraperEquipos__contenido_a_tabla(contenido)

	filas=scraper_equipos._ScraperEquipos__obtenerFilas(tabla)

	contenido_filas=scraper_equipos._ScraperEquipos__obtenerContenidoFilas(filas)

	assert isinstance(contenido_filas, list)

	for contenido_fila in contenido_filas:

		assert len(contenido_fila)==9

def test_scraper_equipos_obtener_data_limpia(scraper_equipos):

	contenido=scraper_equipos._Scraper__realizarPeticion()

	tabla=scraper_equipos._ScraperEquipos__contenido_a_tabla(contenido)

	data_limpia=scraper_equipos._ScraperEquipos__obtenerDataLimpia(tabla)

	assert isinstance(data_limpia, pd.DataFrame)

	time.sleep(60)

@pytest.mark.parametrize(["endpoint"],
	[("url",),("/endpoint",),("/en/players",),("/en/matches",)]
)
def test_scraper_equipos_obtener_equipos_sin_datos(endpoint):

	scraper_equipos=ScraperEquipos(endpoint)

	with pytest.raises(EquiposError):

		scraper_equipos.obtenerEquipos()

def test_scraper_equipos_obtener_equipos(scraper_equipos):

	equipos=scraper_equipos.obtenerEquipos()

	assert isinstance(equipos, pd.DataFrame)