import pytest
from bs4 import BeautifulSoup as bs4
import pandas as pd
import time

from src.scraper_ligas import ScraperLigas
from src.excepciones import PaginaError, LigasError

def test_crear_objeto_scraper_ligas():

	scraper=ScraperLigas("endpoint")

def test_scraper_ligas_realizar_peticion_error(scraper):

	scraper=ScraperLigas("/endpoint")

	with pytest.raises(PaginaError):

		scraper._Scraper__realizarPeticion()

def test_scraper_ligas_realizar_peticion(scraper_ligas):

	contenido=scraper_ligas._Scraper__realizarPeticion()

	assert isinstance(contenido, bs4)

def test_scraper_ligas_obtener_tabla(scraper_ligas):

	contenido=scraper_ligas._Scraper__realizarPeticion()

	tabla=scraper_ligas._ScraperLigas__contenido_a_tabla(contenido)

	assert tabla is not None

def test_scraper_ligas_obtener_columnas(scraper_ligas):

	contenido=scraper_ligas._Scraper__realizarPeticion()

	tabla=scraper_ligas._ScraperLigas__contenido_a_tabla(contenido)

	columnas=scraper_ligas._ScraperLigas__obtenerColumnas(tabla)

	assert isinstance(columnas, list)
	assert len(columnas)==6

def test_scraper_ligas_obtener_filas(scraper_ligas):

	contenido=scraper_ligas._Scraper__realizarPeticion()

	tabla=scraper_ligas._ScraperLigas__contenido_a_tabla(contenido)

	filas=scraper_ligas._ScraperLigas__obtenerFilas(tabla)

	assert isinstance(filas, list)

def test_scraper_ligas_obtener_contenido_filas(scraper_ligas):

	contenido=scraper_ligas._Scraper__realizarPeticion()

	tabla=scraper_ligas._ScraperLigas__contenido_a_tabla(contenido)

	filas=scraper_ligas._ScraperLigas__obtenerFilas(tabla)

	contenido_filas=scraper_ligas._ScraperLigas__obtenerContenidoFilas(filas)

	assert isinstance(contenido_filas, list)

	for contenido_fila in contenido_filas:

		assert len(contenido_fila)==2

def test_scraper_ligas_obtener_data_limpia(scraper_ligas):

	contenido=scraper_ligas._Scraper__realizarPeticion()

	tabla=scraper_ligas._ScraperLigas__contenido_a_tabla(contenido)

	data_limpia=scraper_ligas._ScraperLigas__obtenerDataLimpia(tabla)

	assert isinstance(data_limpia, pd.DataFrame)

	time.sleep(60)

@pytest.mark.parametrize(["endpoint"],
	[("url",),("/endpoint",),("/en/players",),("/en/matches",)]
)
def test_scraper_ligas_obtener_ligas_sin_datos(endpoint):

	scraper_ligas=ScraperLigas(endpoint)

	with pytest.raises(LigasError):

		scraper_ligas.obtenerLigas()

def test_scraper_ligas_obtener_ligas(scraper_ligas):

	ligas=scraper_ligas.obtenerLigas()

	assert isinstance(ligas, pd.DataFrame)