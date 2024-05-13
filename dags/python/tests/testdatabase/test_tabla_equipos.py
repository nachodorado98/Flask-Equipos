import pytest

def test_tabla_equipos_vacia(conexion):

	conexion.c.execute("SELECT * FROM equipos")

	assert not conexion.c.fetchall()

def test_insertar_equipo(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", "url", "atleti", id_liga]

	conexion.insertarEquipo(equipo)

	conexion.c.execute("SELECT * FROM equipos")

	assert len(conexion.c.fetchall())==1

def test_existe_equipo_no_existe(conexion):

	assert not conexion.existe_equipo("Atlético Madrid")

def test_existe_equipo_existe(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", "url", "atleti", id_liga]

	conexion.insertarEquipo(equipo)

	assert conexion.existe_equipo("Atlético Madrid")

def test_actualizar_url_imagen_equipo_no_existe_equipo(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", "url", "atleti", id_liga]

	conexion.insertarEquipo(equipo)

	conexion.c.execute(f"SELECT Url_Imagen FROM equipos")

	url_imagen_inicio=conexion.c.fetchone()["url_imagen"]

	assert url_imagen_inicio is None

	url_imagen="www.imagen.png"

	conexion.actualizarUrlImagen(url_imagen, 1)

	conexion.c.execute(f"SELECT Url_Imagen FROM equipos")

	url_imagen_actualizado=conexion.c.fetchone()["url_imagen"]

	assert url_imagen_actualizado is None

def test_actualizar_url_imagen_equipo_existe_equipo(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", "url", "atleti", id_liga]

	conexion.insertarEquipo(equipo)

	conexion.c.execute(f"SELECT Id, Url_Imagen FROM equipos")

	datos_equipo=conexion.c.fetchone()

	id_equipo, url_imagen_inicio=datos_equipo["id"], datos_equipo["url_imagen"]

	assert url_imagen_inicio is None

	url_imagen="www.imagen.png"

	conexion.actualizarUrlImagen(url_imagen, id_equipo)

	conexion.c.execute(f"SELECT Url_Imagen FROM equipos")

	url_imagen_actualizado=conexion.c.fetchone()["url_imagen"]

	assert url_imagen_actualizado==url_imagen

def test_obtener_id_equipo_no_existe(conexion):

	assert not conexion.obtenerIdEquipo("Atlético Madrid")

def test_obtener_id_equipo_existe(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", "url", "atleti", id_liga]

	conexion.insertarEquipo(equipo)

	id_equipo=conexion.obtenerIdEquipo("Atlético Madrid")

	assert isinstance(id_equipo, int)

def test_obtener_id_url_equipos_no_existen(conexion):

	assert conexion.obtenerIdUrlEquipos() is None

@pytest.mark.parametrize(["equipos", "numero"],
	[
		([["Atlético Madrid", "url", "atleti"]], 1),
		([["Atlético Madrid", "url", "atleti"], ["Rayo", "url", "rayo"]], 2),
		([["Atlético Madrid", "url", "atleti"], ["Rayo", "url", "rayo"], ["Betis", "url", "betis"]], 3),
	]
)
def test_obtener_id_url_equipos_existen(conexion, equipos, numero):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	for equipo in equipos:

		equipo.append(id_liga)

		conexion.insertarEquipo(equipo)

	lista_equipos=conexion.obtenerIdUrlEquipos()

	assert len(lista_equipos)==numero

def test_obtener_url_imagen_equipo_no_existe(conexion):

	assert conexion.obtenerUrlImagen(1) is None

def test_obtener_url_imagen_equipo_existe_no_tiene(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", "url", "atleti", id_liga]

	conexion.insertarEquipo(equipo)

	id_equipo=conexion.obtenerIdEquipo("Atlético Madrid")

	assert conexion.obtenerUrlImagen(id_equipo) is None

def test_obtener_url_imagen_equipo(conexion):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", "url", "atleti", id_liga]

	conexion.insertarEquipo(equipo)

	id_equipo=conexion.obtenerIdEquipo("Atlético Madrid")

	url_imagen="www.imagen.png"

	conexion.actualizarUrlImagen(url_imagen, id_equipo)

	assert conexion.obtenerUrlImagen(id_equipo)==url_imagen

def test_obtener_nombre_equipo_url_no_existe(conexion):

	assert conexion.obtenerNombreEquipoUrl(1) is None

@pytest.mark.parametrize(["nombre"],
	[("atleti",),("nacho",),("atleti-madrid",),("equipo de futbol",)]
)
def test_obtener_nombre_equipo_url(conexion, nombre):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", "url", nombre, id_liga]

	conexion.insertarEquipo(equipo)

	id_equipo=conexion.obtenerIdEquipo("Atlético Madrid")

	nombre_equipo_url=conexion.obtenerNombreEquipoUrl(id_equipo)

	assert nombre_equipo_url==nombre

@pytest.mark.parametrize(["codigo"],
	[("1235",),("54codigo",),("url2",),("url12345codig0",)]
)
def test_comprobar_codigo_no_existe(conexion, codigo):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", "url12345codigo", "Atleti", id_liga]

	conexion.insertarEquipo(equipo)

	assert not conexion.comprobarCodigo(codigo)

@pytest.mark.parametrize(["codigo"],
	[("12345",),("45codigo",),("url1",),("url12345codig",)]
)
def test_comprobar_codigo(conexion, codigo):

	liga=["España", "url", "ESP"]

	conexion.insertarLiga(liga)

	id_liga=conexion.obtenerIdLiga("España")

	equipo=["Atlético Madrid", "url12345codigo", "Atleti", id_liga]

	conexion.insertarEquipo(equipo)

	assert conexion.comprobarCodigo(codigo)