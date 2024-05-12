import pytest
import pandas as pd
import time
from datetime import datetime, timedelta

from src.etl_partidos import extraerDataPartidos
from src.excepciones import ErrorFechaFormato, ErrorFechaPosterior, PartidosExtraidosError

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