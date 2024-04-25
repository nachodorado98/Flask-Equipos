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