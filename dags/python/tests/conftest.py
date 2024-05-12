import os
import sys
sys.path.append(os.path.abspath(".."))

import pytest

from src.scraper import Scraper
from src.scraper_ligas import ScraperLigas
from src.config import ENDPOINT_LIGAS
from src.database.conexion import Conexion
from src.scraper_equipos import ScraperEquipos
from src.scraper_info_equipo import ScraperInfoEquipo
from src.datalake.conexion_data_lake import ConexionDataLake
from src.scraper_partidos import ScraperPartidos

@pytest.fixture
def scraper():

	return Scraper(ENDPOINT_LIGAS)

@pytest.fixture
def scraper_ligas():

	return ScraperLigas(ENDPOINT_LIGAS)

@pytest.fixture(scope="function")
def conexion():

    con=Conexion()

    con.c.execute("DELETE FROM ligas")

    con.confirmar()

    return con

@pytest.fixture
def scraper_equipos():

    return ScraperEquipos("/en/country/clubs/ESP/Spain-Football-Clubs")

@pytest.fixture
def scraper_info_equipo():

    return ScraperInfoEquipo("/en/squads/db3b9613/history/Atletico-Madrid-Stats-and-History")

@pytest.fixture(scope="function")
def datalake():

    return ConexionDataLake()

@pytest.fixture
def scraper_partidos():

    return ScraperPartidos("2019-04-13")