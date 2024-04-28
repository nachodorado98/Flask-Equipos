import pytest

def test_tabla_ligas_vacia(conexion):

	conexion.c.execute("SELECT * FROM ligas")

	assert not conexion.c.fetchall()

def test_insertar_liga(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	conexion.c.execute("SELECT * FROM ligas")

	assert len(conexion.c.fetchall())==1

def test_existe_liga_no_existe(conexion):

	assert not conexion.existe_liga("España")

def test_existe_liga_existe(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	assert conexion.existe_liga("España")

def test_obtener_id_liga_no_existe(conexion):

	assert not conexion.obtenerIdLiga("España")

def test_obtener_id_liga_existe(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	assert isinstance(id_liga, int)

def test_obtener_id_url_ligas_no_existen(conexion):

	assert conexion.obtenerIdUrlLigas() is None

@pytest.mark.parametrize(["ligas", "numero"],
	[
		([["España", "url", "ESP"]], 1),
		([["España", "url", "ESP"],["Francia", "urlf", "F"]], 2),
		([["España", "url", "ESP"],["Francia", "urlf", "F"], ["Liga", "liga", "l"]], 3),
	]
)
def test_obtener_id_url_ligas_existen(conexion, ligas, numero):

	for liga in ligas:

		conexion.insertarLiga(liga)

	lista_ligas=conexion.obtenerIdUrlLigas()

	assert len(lista_ligas)==numero