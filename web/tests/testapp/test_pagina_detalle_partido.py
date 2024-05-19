def anadirPartidos(conexion):

	partidos=[["Champions", "Final", "2019-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
			["Champions", "Final", "2020-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
			["Champions", "Final", "2019-07-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
			["Champions", "Final", "2019-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
			["Champions", "Final", "2023-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
			["Champions", "Final", "2022-06-13","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],
			["Champions", "Final", "2019-06-22","21:00", "ATM", "5-0", "Madrid", 12345, "Calderon", "Cod1", "Cod2"],]

	for partido in partidos:

		conexion.c.execute("""INSERT INTO partidos (competicion, ronda, fecha, hora,
								local, marcador, visitante, publico, sede, codequipo1, codequipo2)
								VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
								tuple(partido))

		conexion.confirmar()

def test_pagina_detalle_partido_no_existe(cliente, conexion):

	respuesta=cliente.get("/detalle_partido/0")

	contenido=respuesta.data.decode()

	respuesta.status_code==302
	assert respuesta.location=="/"
	assert "Redirecting..." in contenido

def test_pagina_detalle_partido(cliente, conexion):

	anadirPartidos(conexion)

	partidos_obtenidos=conexion.obtenerPartidosFecha("2019-06-22")

	id_partido=partidos_obtenidos[0][-1]

	respuesta=cliente.get(f"/detalle_partido/{id_partido}")

	contenido=respuesta.data.decode()

	respuesta.status_code==200
	assert "Fecha: " in contenido
	assert "personas" in contenido