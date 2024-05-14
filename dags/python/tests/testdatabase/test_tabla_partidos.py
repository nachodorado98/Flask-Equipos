import pytest

def test_tabla_partidos_vacia(conexion):

	conexion.c.execute("SELECT * FROM partidos")

	assert not conexion.c.fetchall()

def test_insertar_partido(conexion):

	partido=["Champions", "Final", "2019-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"]

	conexion.insertarPartido(partido)

	conexion.c.execute("SELECT * FROM partidos")

	assert len(conexion.c.fetchall())==1

@pytest.mark.parametrize(["numero_partidos"],
	[(2,),(10,),(5,),(13,),(22,)]
)
def test_insertar_partidos(conexion, numero_partidos):

	partidos=[["Champions", "Final", "2019-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"] for _ in range(numero_partidos)]

	conexion.insertarPartidos(partidos)

	conexion.c.execute("SELECT * FROM partidos")

	assert len(conexion.c.fetchall())==numero_partidos

def test_tabla_vacia(conexion):

	assert conexion.tabla_vacia()

def test_tabla_no_vacia(conexion):

	partido=["Champions", "Final", "2019-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"]

	conexion.insertarPartido(partido)

	assert not conexion.tabla_vacia()

def test_fecha_maxima_vacia(conexion):

	assert conexion.fecha_maxima() is None

def test_fecha_maxima_registro(conexion):

	partido=["Champions", "Final", "2019-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"]

	conexion.insertarPartido(partido)

	assert conexion.fecha_maxima()=="2019-06-22"

@pytest.mark.parametrize(["numero_partidos"],
	[(2,),(10,),(5,),(13,),(22,)]
)
def test_fecha_maxima_registros(conexion, numero_partidos):

	partidos=[["Champions", "Final", "2019-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"] for _ in range(numero_partidos)]

	conexion.insertarPartidos(partidos)

	assert conexion.fecha_maxima()=="2019-06-22"

def test_fecha_maxima_registros_diferentes_fechas(conexion):

	partidos=[["Champions", "Final", "2019-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2020-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2019-07-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2023-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
				["Champions", "Final", "2022-06-13","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"]]

	conexion.insertarPartidos(partidos)

	assert conexion.fecha_maxima()=="2023-06-22"