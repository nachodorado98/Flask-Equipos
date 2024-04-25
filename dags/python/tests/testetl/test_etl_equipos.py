import pytest
import pandas as pd
import time

from src.etl_equipos import extraerDataEquipos, limpiarDataEquipos, cargarDataEquipos
from src.excepciones import EquiposError, EquiposExistentesError

def test_extraer_data_equipos_error_endpoint():

	with pytest.raises(EquiposError):

		extraerDataEquipos("/en/players")

def test_extraer_data_equipos():

	data=extraerDataEquipos("/en/country/clubs/ESP/Spain-Football-Clubs")

	assert isinstance(data, pd.DataFrame)
	assert not data.empty

def test_limpiar_data_equipos_error():

	data=extraerDataEquipos("/en/country/clubs/GUF/French-Guiana-Football-Clubs")

	with pytest.raises(EquiposExistentesError):

		limpiarDataEquipos(data)

def test_limpiar_data_equipos():

	data=extraerDataEquipos("/en/country/clubs/ESP/Spain-Football-Clubs")

	data_limpia=limpiarDataEquipos(data)

	assert isinstance(data_limpia, pd.DataFrame)
	assert not data_limpia.empty
	assert len(data_limpia.columns)==3

def test_cargar_data_equipos(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	data=extraerDataEquipos("/en/country/clubs/ESP/Spain-Football-Clubs")

	data_limpia=limpiarDataEquipos(data)

	cargarDataEquipos(data_limpia, id_liga)

	conexion.c.execute("SELECT * FROM equipos")

	assert len(conexion.c.fetchall())==data_limpia.shape[0]

def test_cargar_data_existe_equipo(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	data=extraerDataEquipos("/en/country/clubs/ESP/Spain-Football-Clubs")

	data_limpia=limpiarDataEquipos(data)

	cargarDataEquipos(data_limpia, id_liga)

	conexion.c.execute("SELECT * FROM equipos")

	equipos1=len(conexion.c.fetchall())

	data_2=extraerDataEquipos("/en/country/clubs/ESP/Spain-Football-Clubs")

	data_limpia_2=limpiarDataEquipos(data_2)

	cargarDataEquipos(data_limpia_2, id_liga)

	conexion.c.execute("SELECT * FROM equipos")

	equipos2=len(conexion.c.fetchall())

	assert equipos1==equipos2

def test_cargar_data_nuevo_equipo(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	data=extraerDataEquipos("/en/country/clubs/ESP/Spain-Football-Clubs")

	data_limpia=limpiarDataEquipos(data)

	data_limpia_dropeada=data_limpia.drop(0)

	assert data_limpia.shape[0]>data_limpia_dropeada.shape[0]

	cargarDataEquipos(data_limpia_dropeada, id_liga)

	conexion.c.execute("SELECT * FROM equipos")

	equipos1=len(conexion.c.fetchall())

	data_2=extraerDataEquipos("/en/country/clubs/ESP/Spain-Football-Clubs")

	data_limpia_2=limpiarDataEquipos(data_2)

	cargarDataEquipos(data_limpia_2, id_liga)

	conexion.c.execute("SELECT * FROM equipos")

	equipos2=len(conexion.c.fetchall())

	assert equipos2>equipos1

	time.sleep(60)