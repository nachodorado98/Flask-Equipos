import pytest
from bs4 import BeautifulSoup as bs4
import time

from src.scraper_info_equipo import ScraperInfoEquipo
from src.excepciones import PaginaError, InfoEquipoError

def test_crear_objeto_scraper_info_equipo():

	scraper=ScraperInfoEquipo("endpoint")

def test_scraper_info_equipo_realizar_peticion_error(scraper):

	scraper=ScraperInfoEquipo("/endpoint")

	with pytest.raises(PaginaError):

		scraper._Scraper__realizarPeticion()

def test_scraper_info_equipo_realizar_peticion(scraper_info_equipo):

	contenido=scraper_info_equipo._Scraper__realizarPeticion()

	assert isinstance(contenido, bs4)

def test_scraper_info_equipo_obtener_info(scraper_info_equipo):

	contenido=scraper_info_equipo._Scraper__realizarPeticion()

	info=scraper_info_equipo._ScraperInfoEquipo__contenido_info(contenido)

	assert info is not None

def test_scraper_info_equipo_obtener_url_escudo(scraper_info_equipo):

	contenido=scraper_info_equipo._Scraper__realizarPeticion()

	info=scraper_info_equipo._ScraperInfoEquipo__contenido_info(contenido)

	url_escudo=scraper_info_equipo._ScraperInfoEquipo__obtenerUrlEscudo(info)

	assert isinstance(url_escudo, str)

	time.sleep(60)

@pytest.mark.parametrize(["endpoint"],
	[("url",),("/endpoint",),("/en/players",),("/en/matches",)]
)
def test_scraper_info_equipo_obtener_info_equipo_sin_datos(endpoint):

	scraper_info_equipo=ScraperInfoEquipo(endpoint)

	with pytest.raises(InfoEquipoError):

		scraper_info_equipo.obtenerInfo()

def test_scraper_info_equipo_obtener_info_equipo(scraper_info_equipo):

	url_escudo=scraper_info_equipo.obtenerInfo()

	assert isinstance(url_escudo, str)