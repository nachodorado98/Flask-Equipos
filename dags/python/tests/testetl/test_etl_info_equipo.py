import pytest
import time

from src.etl_info_equipo import extraerDataInfoEquipo, limpiarDataInfoEquipo, cargarDataInfoEquipo
from src.excepciones import InfoEquipoError, UrlImagenExistenteError

def test_extraer_data_info_equipo_error_endpoint():

	with pytest.raises(InfoEquipoError):

		extraerDataInfoEquipo("/en/players")

def test_extraer_data_info_equipo():

	data=extraerDataInfoEquipo("/en/squads/db3b9613/history/Atletico-Madrid-Stats-and-History")

	assert isinstance(data, str)

@pytest.mark.parametrize(["url_imagen"],
	[("",),("A",),("url",),("https://cdn.ssref.net/",),("https://cdn.ssref.net/imagen",),("https://cdn.ssref.net/imagen.com",)]
)
def test_limpiar_data_info_equipo_error(url_imagen):

	with pytest.raises(UrlImagenExistenteError):

		limpiarDataInfoEquipo(url_imagen)

@pytest.mark.parametrize(["url_equipo"],
	[
		("/en/squads/db3b9613/history/Atletico-Madrid-Stats-and-History",),
		("/en/squads/054efa67/history/Bayern-Munich-Stats-and-History",),
		("/en/squads/e2d8892c/history/Paris-Saint-Germain-Stats-and-History",),
		("/en/squads/822bd0ba/history/Liverpool-Stats-and-History",),
		("/en/squads/19538871/history/Manchester-United-Stats-and-History",),
		("/en/squads/206d90db/history/Barcelona-Stats-and-History",),
		("/en/squads/b8fd03ef/history/Manchester-City-Stats-and-History",),
	]
)
def test_limpiar_data_info_equipo(url_equipo):

	data=extraerDataInfoEquipo(url_equipo)

	url_limpia=limpiarDataInfoEquipo(data)

	assert isinstance(url_limpia, str)
	assert url_limpia==data

def test_cargar_data_info_equipo(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", "url", "atleti", id_liga]

	conexion.insertarEquipo(equipo)

	id_equipo=conexion.obtenerIdEquipo("Atlético Madrid")

	conexion.c.execute(f"SELECT Url_Imagen FROM equipos WHERE id={id_equipo}")

	assert conexion.c.fetchone()["url_imagen"] is None

	data=extraerDataInfoEquipo("/en/squads/db3b9613/history/Atletico-Madrid-Stats-and-History")

	url_limpia=limpiarDataInfoEquipo(data)

	cargarDataInfoEquipo(url_limpia, id_equipo)

	conexion.c.execute(f"SELECT Url_Imagen FROM equipos WHERE id={id_equipo}")

	assert conexion.c.fetchone()["url_imagen"] is not None

	time.sleep(60)