import pytest
from datetime import datetime, timedelta

from src.utilidades.utils import fecha_formato_correcto, fecha_anterior, fecha_siguiente, fecha_bonita
from src.utilidades.utils import es_maxima, es_minima, fecha_disponible

@pytest.mark.parametrize(["fecha"],
	[("fecha",),("1111",),("2019-01-111",),("2011-0216",),("22062019",),("2019/06/22",)]
)
def test_fecha_formato_incorrecto(fecha):

	assert not fecha_formato_correcto(fecha)

@pytest.mark.parametrize(["fecha"],
	[("2019-01-11",),("2011-02-16",),("2019-04-13",),("2019-06-22",)]
)
def test_fecha_formato_correcto(fecha):

	assert fecha_formato_correcto(fecha)

@pytest.mark.parametrize(["fecha", "dia_anterior"],
	[
		("2019-04-13", "2019-04-12"),
		("2019-06-22", "2019-06-21"),
		("2020-01-01", "2019-12-31"),
		("2019-06-01", "2019-05-31"),
		("2024-03-01", "2024-02-29")
	]
)
def test_fecha_anterior(fecha, dia_anterior):

	assert fecha_anterior(fecha)==dia_anterior

@pytest.mark.parametrize(["fecha", "dia_siguiente"],
	[
		("2019-04-12", "2019-04-13"),
		("2019-06-21", "2019-06-22"),
		("2019-12-31", "2020-01-01"),
		("2019-05-31", "2019-06-01"),
		("2024-02-29", "2024-03-01")
	]
)
def test_fecha_siguiente(fecha, dia_siguiente):

	assert fecha_siguiente(fecha)==dia_siguiente

@pytest.mark.parametrize(["fecha", "fecha_extendida"],
	[
		("2019-01-11", "Viernes 11 de Enero de 2019"),
		("2020-02-16", "Domingo 16 de Febrero de 2020"),
		("2019-04-13", "Sábado 13 de Abril de 2019"),
		("2019-06-22", "Sábado 22 de Junio de 2019")
	]
)
def test_fecha_bonita(fecha, fecha_extendida):

	assert fecha_bonita(fecha)==fecha_extendida

@pytest.mark.parametrize(["fecha"],
	[("2019-04-13",),("2019-06-22",),("2020-01-01",),("2019-06-01",),("2024-03-01",)]
)
def test_fecha_no_es_maxima(fecha):

	fecha_maxima="2024-03-05"

	assert not es_maxima(fecha, fecha_maxima)

def test_fecha_es_maxima():

	fecha_maxima="2024-03-05"

	assert es_maxima(fecha_maxima, fecha_maxima)

@pytest.mark.parametrize(["fecha"],
	[("2019-04-13",),("2019-06-22",),("2020-01-01",),("2019-06-01",),("2024-03-01",)]
)
def test_fecha_no_es_minima(fecha):

	fecha_minima="2019-03-05"

	assert not es_minima(fecha, fecha_minima)

def test_fecha_es_minima():

	fecha_minima="2019-03-05"

	assert es_minima(fecha_minima, fecha_minima)

def test_fecha_disponible_fecha_minima():

	fecha_minima="2019-03-05"
	fecha_maxima="2024-03-05"

	assert fecha_disponible(fecha_minima, fecha_minima, fecha_maxima)

def test_fecha_disponible_fecha_maxima():

	fecha_minima="2019-03-05"
	fecha_maxima="2024-03-05"

	assert fecha_disponible(fecha_maxima, fecha_minima, fecha_maxima)

@pytest.mark.parametrize(["dias"],
	[(1,),(2,),(22,),(13,),(200,)]
)
def test_fecha_disponible_inferior_fecha_minima(dias):

	fecha_minima="2019-03-05"
	fecha_maxima="2024-03-05"

	fecha=(datetime.strptime(fecha_minima, "%Y-%m-%d")-timedelta(days=dias)).strftime("%Y-%m-%d")

	assert not fecha_disponible(fecha, fecha_minima, fecha_maxima)

@pytest.mark.parametrize(["dias"],
	[(1,),(2,),(22,),(13,),(200,)]
)
def test_fecha_disponible_superior_fecha_maxima(dias):

	fecha_minima="2019-03-05"
	fecha_maxima="2024-03-05"

	fecha=(datetime.strptime(fecha_maxima, "%Y-%m-%d")+timedelta(days=dias)).strftime("%Y-%m-%d")

	assert not fecha_disponible(fecha, fecha_minima, fecha_maxima)

@pytest.mark.parametrize(["dias"],
	[(1,),(2,),(22,),(13,),(200,)]
)
def test_fecha_disponible_superior_fecha_minima(dias):

	fecha_minima="2019-03-05"
	fecha_maxima="2024-03-05"

	fecha=(datetime.strptime(fecha_minima, "%Y-%m-%d")+timedelta(days=dias)).strftime("%Y-%m-%d")

	assert fecha_disponible(fecha, fecha_minima, fecha_maxima)

@pytest.mark.parametrize(["dias"],
	[(1,),(2,),(22,),(13,),(200,)]
)
def test_fecha_disponible_inferior_fecha_maxima(dias):

	fecha_minima="2019-03-05"
	fecha_maxima="2024-03-05"

	fecha=(datetime.strptime(fecha_maxima, "%Y-%m-%d")-timedelta(days=dias)).strftime("%Y-%m-%d")

	assert fecha_disponible(fecha, fecha_minima, fecha_maxima)