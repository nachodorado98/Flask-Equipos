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