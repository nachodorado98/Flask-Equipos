import pytest
import pandas as pd

from src.etl import extraerData, limpiarData
from src.excepciones import LigasError, LigasExistentesError

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
	assert not data.empty
	assert len(data_limpia)==len(ligas)

	