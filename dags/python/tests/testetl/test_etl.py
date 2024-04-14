import pytest
import pandas as pd
import time

from src.etl import extraerData, limpiarData, cargarData
from src.excepciones import LigasError, LigasExistentesError, LigaCargadaError

def test_extraer_data_error_endpoint():

	with pytest.raises(LigasError):

		extraerData("/en/players")

def test_extraer_data():

	data=extraerData()

	assert isinstance(data, pd.DataFrame)
	assert not data.empty

def test_limpiar_data_error():

	data=extraerData()

	with pytest.raises(LigasExistentesError):

		limpiarData(data, [])

@pytest.mark.parametrize(["ligas"],
	[(["Spain", "France", "Italy"],),(["Spain", "Italy"],),(["Spain", "France", "Italy", "Great Britain"],)]
)
def test_limpiar_data(ligas):

	data=extraerData()

	data_limpia=limpiarData(data, ligas)

	assert isinstance(data_limpia, pd.DataFrame)
	assert not data_limpia.empty
	assert len(data_limpia)==len(ligas)

@pytest.mark.parametrize(["ligas"],
	[(["Spain", "France", "Italy"],),(["Spain", "Italy"],),(["Spain", "France", "Italy", "Great Britain"],)]
)
def test_cargar_data(conexion, ligas):

	data=extraerData()

	data_limpia=limpiarData(data, ligas)

	cargarData(data_limpia)

	conexion.c.execute("SELECT * FROM ligas")

	assert len(conexion.c.fetchall())==len(ligas)

def test_cargar_data_existe_liga(conexion):

	data=extraerData()

	data_limpia=limpiarData(data)

	cargarData(data_limpia)

	data_2=extraerData()

	data_limpia_2=limpiarData(data_2)

	with pytest.raises(LigaCargadaError):

		cargarData(data_limpia_2)

	time.sleep(60)