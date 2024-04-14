import pytest
import pandas as pd

from src.etl_equipos import extraerDataEquipos, limpiarDataEquipos
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