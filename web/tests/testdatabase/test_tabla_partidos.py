def test_tabla_partidos_vacia_inicio(conexion):

	conexion.c.execute("SELECT * FROM partidos")

	assert not conexion.c.fetchall()

def test_tabla_partidos_llena(conexion):

	assert conexion.tabla_partidos_vacia()

def test_obtener_partidos_no_existen(conexion):

	assert conexion.obtenerPartidosFecha("2019-06-22") is None

def test_obtener_partidos_existen(conexion):

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

	assert not conexion.tabla_partidos_vacia()

	partidos_obtenidos=conexion.obtenerPartidosFecha("2019-06-22")

	assert len(partidos_obtenidos)==3

	for partido in partidos_obtenidos:

		assert partido[5]=="22-06-2019"

	conexion.c.execute("DELETE FROM partidos")

	conexion.confirmar()