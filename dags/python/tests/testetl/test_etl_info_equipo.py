import pytest

from src.etl_info_equipo import extraerDataInfoEquipo
from src.excepciones import InfoEquipoError

def test_extraer_data_info_equipo_error_endpoint():

	with pytest.raises(InfoEquipoError):

		extraerDataInfoEquipo("/en/players")

def test_extraer_data_info_equipo():

	data=extraerDataInfoEquipo("/en/squads/db3b9613/history/Atletico-Madrid-Stats-and-History")

	assert isinstance(data, str)