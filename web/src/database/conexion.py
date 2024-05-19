import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, List

from .confconexion import *

# Clase para la conexion a la BBDD
class Conexion:

	def __init__(self)->None:

		try:
			
			self.bbdd=psycopg2.connect(host=HOST, user=USUARIO, password=CONTRASENA, port=PUERTO, database=BBDD)
			self.c=self.bbdd.cursor(cursor_factory=RealDictCursor)

		except psycopg2.OperationalError as e:

			print("Error en la conexion a la BBDD")

	# Metodo para cerrar la conexion a la BBDD
	def cerrarConexion(self)->None:

		self.c.close()
		self.bbdd.close()

	# Metodo para confirmar una accion
	def confirmar(self)->None:

		self.bbdd.commit()

	# Metodo para obtener la fecha maxima de la tabla
	def fecha_maxima(self)->Optional[str]:

		self.c.execute("""SELECT MAX(fecha) as fecha_maxima
							FROM partidos""")

		fecha_maxima=self.c.fetchone()["fecha_maxima"]

		return None if fecha_maxima is None else fecha_maxima.strftime("%Y-%m-%d")

	# Metodo para obtener los partidos de una fecha
	def obtenerPartidosFecha(self, fecha:str)->Optional[List[tuple]]:

		self.c.execute("""SELECT p.Id, p.Competicion, p.Ronda, p.Local, p.Marcador, p.Visitante, p.Fecha, 
								CASE WHEN e1.Equipo_url IS NULL THEN 'no-imagen' ELSE e1.Equipo_url END as Local_imagen, 
								CASE WHEN e2.Equipo_url IS NULL THEN 'no-imagen' ELSE e2.Equipo_url END as Visitante_imagen
							FROM partidos p
							LEFT JOIN equipos e1
							ON p.CodEquipo1=e1.Codigo_url 
							LEFT JOIN equipos e2
							ON p.CodEquipo2=e2.Codigo_url
							WHERE p.Fecha=%s
							ORDER BY p.Id""",
							(fecha,))

		partidos=self.c.fetchall()

		return list(map(lambda partido: (partido["competicion"],
											partido["ronda"],
											partido["local"],
											partido["marcador"],
											partido["visitante"],
											partido["fecha"].strftime("%d-%m-%Y"),
											partido["local_imagen"],
											partido["visitante_imagen"],
											partido["id"]), partidos)) if partidos else None

	# Metodo para saber si la tabla partidos esta vacia
	def tabla_partidos_vacia(self)->bool:

		self.c.execute("""SELECT *
						FROM partidos""")

		return True if not self.c.fetchall() else False

	# Metodo para obtener la fecha minima de la tabla
	def fecha_minima(self)->Optional[str]:

		self.c.execute("""SELECT MIN(fecha) as fecha_minima
							FROM partidos""")

		fecha_minima=self.c.fetchone()["fecha_minima"]

		return None if fecha_minima is None else fecha_minima.strftime("%Y-%m-%d")

	# Metodo para comprobar si un partido existe por su id
	def partido_existe(self, id_partido:int)->bool:

		self.c.execute("""SELECT *
						FROM partidos
						WHERE Id=%s""",
						(id_partido,))

		return False if not self.c.fetchone() else True

	# Metodo para obtener el detalle de un partido
	def detalle_partido(self, id_partido:int)->Optional[tuple]:

		self.c.execute("""SELECT p.Competicion, p.Ronda, p.Local, p.Marcador, p.Visitante, p.Fecha, 
								CASE WHEN e1.Equipo_url IS NULL THEN 'no-imagen' ELSE e1.Equipo_url END as Local_imagen, 
								CASE WHEN e2.Equipo_url IS NULL THEN 'no-imagen' ELSE e2.Equipo_url END as Visitante_imagen,
								p.Hora, p.Publico, p.Sede
							FROM partidos p
							LEFT JOIN equipos e1
							ON p.CodEquipo1=e1.Codigo_url 
							LEFT JOIN equipos e2
							ON p.CodEquipo2=e2.Codigo_url
							WHERE p.Id=%s""",
							(id_partido,))

		partido=self.c.fetchone()

		return None if partido is None else (partido["competicion"],
											partido["ronda"],
											partido["local"],
											partido["marcador"],
											partido["visitante"],
											partido["fecha"].strftime("%d-%m-%Y"),
											partido["local_imagen"],
											partido["visitante_imagen"],
											partido["hora"],
											partido["publico"],
											partido["sede"])