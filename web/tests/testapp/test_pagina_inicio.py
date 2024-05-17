import pytest
from datetime import datetime

from src.utilidades.utils import fecha_bonita

def test_pagina_inicial_sin_partidos(cliente, conexion):

	respuesta=cliente.get("/")

	contenido=respuesta.data.decode()

	respuesta.status_code==200
	assert "No se han encontrados datos de partidos." in contenido
	assert "Ejecuta el DAG de los Partidos en Apache Airflow" in contenido

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

@pytest.mark.parametrize(["fecha"],
	[("fecha",),("1111",),("2019-01-111",),("2011-0216",),("22062019",),("2019/06/22",)]
)
def test_pagina_inicial_formato_incorrecto(cliente, conexion, fecha):

	anadirPartidos(conexion)

	assert not conexion.tabla_partidos_vacia()	

	respuesta=cliente.get(f"/?fecha={fecha}")

	contenido=respuesta.data.decode()

	respuesta.status_code==302
	assert respuesta.location=="/"
	assert "Redirecting..." in contenido

def test_pagina_inicial_formato_correcto(cliente):

	respuesta=cliente.get("/?fecha=2019-06-22")

	contenido=respuesta.data.decode()

	respuesta.status_code==200
	assert "No hay partidos este dia" not in contenido
	assert "Partidos Del Sábado 22 de Junio de 2019" in contenido

@pytest.mark.parametrize(["fecha"],
	[
		("2019-06-20",),
		("2023-02-01",),
		("2022-02-22",)
	]
)
def test_pagina_inicial_partidos_no_existen(cliente, fecha):

	respuesta=cliente.get(f"/?fecha={fecha}")

	contenido=respuesta.data.decode()

	respuesta.status_code==200
	assert "No hay partidos este dia" in contenido
	assert "Partidos Del "+fecha_bonita(fecha) in contenido

def test_pagina_inicial_fecha_maxima(cliente, conexion):

	anadirPartidos(conexion)

	assert not conexion.tabla_partidos_vacia()	

	fecha_maxima=conexion.fecha_maxima()

	respuesta=cliente.get("/")

	contenido=respuesta.data.decode()

	respuesta.status_code==200
	assert "Partidos Del "+fecha_bonita(fecha_maxima) in contenido
	assert 'class="boton-anterior"' in contenido
	assert 'class="boton-anterior boton-anterior-deshabilitado"' not in contenido
	assert 'class="boton-siguiente"' not in contenido
	assert 'class="boton-siguiente boton-siguiente-deshabilitado"' in contenido

def test_pagina_inicial_fecha_minima(cliente, conexion):

	anadirPartidos(conexion)

	assert not conexion.tabla_partidos_vacia()	

	fecha_minima=conexion.fecha_minima()

	respuesta=cliente.get(f"/?fecha={fecha_minima}")

	contenido=respuesta.data.decode()

	respuesta.status_code==200
	assert "Partidos Del "+fecha_bonita(fecha_minima) in contenido
	assert 'class="boton-anterior"' not in contenido
	assert 'class="boton-anterior boton-anterior-deshabilitado"' in contenido
	assert 'class="boton-siguiente"' in contenido
	assert 'class="boton-siguiente boton-siguiente-deshabilitado"' not in contenido