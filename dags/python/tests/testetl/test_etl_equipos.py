import pytest
import pandas as pd

from src.etl_equipos import extraerDataEquipos
from src.excepciones import EquiposError

def test_extraer_data_equipos_error_endpoint():

	with pytest.raises(EquiposError):

		extraerDataEquipos("/en/players")

def test_extraer_data_equipos():

	data=extraerDataEquipos("/en/country/clubs/ESP/Spain-Football-Clubs")

	assert isinstance(data, pd.DataFrame)
	assert not data.empty

	print(data)