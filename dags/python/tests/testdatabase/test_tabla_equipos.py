def test_tabla_equipos_vacia(conexion):

	conexion.c.execute("SELECT * FROM equipos")

	assert not conexion.c.fetchall()

def test_existe_equipo_no_existe(conexion):

	assert not conexion.existe_equipo("Atlético Madrid")

def test_existe_equipo_existe(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", "url", "atleti", id_liga]

	conexion.insertarEquipo(equipo)

	assert conexion.existe_equipo("Atlético Madrid")