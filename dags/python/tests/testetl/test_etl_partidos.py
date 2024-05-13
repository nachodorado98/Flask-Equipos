import pytest
import pandas as pd
import time
from datetime import datetime, timedelta

from src.etl_partidos import extraerDataPartidos, limpiarDataPartidos, cargarDataPartidos
from src.excepciones import ErrorFechaFormato, ErrorFechaPosterior, PartidosExtraidosError, PartidosLimpiarError
from src.database.conexion import Conexion

@pytest.mark.parametrize(["fecha"],
	[("201906-22",), ("22/06/2019",), ("22062019",), ("2019-0622",), ("2019-06/22",)]
)
def test_extraer_data_partidos_error_fecha_formato(fecha):

	with pytest.raises(ErrorFechaFormato):

		extraerDataPartidos(fecha)

@pytest.mark.parametrize(["dias"],
	[(1,), (10,), (100,), (1000,), (10000,)]
)
def test_extraer_data_partidos_error_fecha_posterior(dias):

	fecha=(datetime.now() + timedelta(days=dias)).strftime("%Y-%m-%d")

	with pytest.raises(ErrorFechaPosterior):

		extraerDataPartidos(fecha)

def test_extraer_data_partidos_error_partidos():

	with pytest.raises(PartidosExtraidosError):

		extraerDataPartidos("1800-01-01")

def test_extraer_data_partidos():

	data=extraerDataPartidos("2019-06-22")

	assert isinstance(data, pd.DataFrame)
	assert not data.empty

def test_limpiar_partidos_error_no_partidos():

	data=extraerDataPartidos("2019-06-22")

	with pytest.raises(PartidosLimpiarError):

		limpiarDataPartidos(data)

@pytest.mark.parametrize(["url"],
	[("361ca564",), ("codigo361ca564",), ("/361ca564/tottenham",),("/en/squads/361ca564/history/Tottenham-Hotspur-Stats-and-History",)]
)
def test_limpiar_partidos_equipo_local_partido(conexion, url):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", url, "Atleti", id_liga]

	conexion.insertarEquipo(equipo)

	data=extraerDataPartidos("2019-04-13")

	data_limpia=limpiarDataPartidos(data)

	assert isinstance(data_limpia, pd.DataFrame)
	assert not data_limpia.empty
	assert data_limpia.shape[0]==1

@pytest.mark.parametrize(["url"],
	[("f5922ca5",), ("codigof5922ca5",), ("/f5922ca5/equipo",),("/en/squads/f5922ca5/history/Huddersfield-Town-Stats-and-History",)]
)
def test_limpiar_partidos_equipo_visitante_partido(conexion, url):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", url, "Atleti", id_liga]

	conexion.insertarEquipo(equipo)

	data=extraerDataPartidos("2019-04-13")

	data_limpia=limpiarDataPartidos(data)

	assert isinstance(data_limpia, pd.DataFrame)
	assert not data_limpia.empty
	assert data_limpia.shape[0]==1

def test_limpiar_partidos_ambos_equipos(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	urls=["/en/squads/361ca564/history/Tottenham-Hotspur-Stats-and-History",
			"/en/squads/f5922ca5/history/Huddersfield-Town-Stats-and-History"]

	for url in urls:

		equipo=["Atlético Madrid", url, "Atleti", id_liga]

		conexion.insertarEquipo(equipo)

	data=extraerDataPartidos("2019-04-13")

	data_limpia=limpiarDataPartidos(data)

	assert isinstance(data_limpia, pd.DataFrame)
	assert not data_limpia.empty
	assert data_limpia.shape[0]==1

def test_limpiar_partidos_varios_equipos(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	urls=["/en/squads/361ca564/history/Tottenham-Hotspur-Stats-and-History",
			"/en/squads/f5922ca5/history/Huddersfield-Town-Stats-and-History",
			"/en/squads/19538871/history/Manchester-United-Stats-and-History",
			"/en/squads/db3b9613/history/Atletico-Madrid-Stats-and-History",
			"/en/squads/206d90db/history/Barcelona-Stats-and-History"]

	for url in urls:

		equipo=["Atlético Madrid", url, "Atleti", id_liga]

		conexion.insertarEquipo(equipo)

	data=extraerDataPartidos("2019-04-13")

	data_limpia=limpiarDataPartidos(data)

	assert isinstance(data_limpia, pd.DataFrame)
	assert not data_limpia.empty
	assert data_limpia.shape[0]==4

def test_cargar_partidos(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	urls=["/en/squads/361ca564/history/Tottenham-Hotspur-Stats-and-History",
			"/en/squads/f5922ca5/history/Huddersfield-Town-Stats-and-History",
			"/en/squads/19538871/history/Manchester-United-Stats-and-History",
			"/en/squads/db3b9613/history/Atletico-Madrid-Stats-and-History",
			"/en/squads/206d90db/history/Barcelona-Stats-and-History"]

	for url in urls:

		equipo=["Atlético Madrid", url, "Atleti", id_liga]

		conexion.insertarEquipo(equipo)

	data=extraerDataPartidos("2019-04-13")

	data_limpia=limpiarDataPartidos(data)

	cargarDataPartidos(data_limpia)

	conexion.c.execute("SELECT * FROM partidos")

	assert len(conexion.c.fetchall())==4

	time.sleep(60)