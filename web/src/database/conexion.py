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

		self.c.execute("""SELECT competicion, ronda, local, marcador, visitante, fecha
						FROM partidos
						WHERE fecha=%s""",
						(fecha,))

		partidos=self.c.fetchall()

		return list(map(lambda partido: (partido["competicion"],
											partido["ronda"],
											partido["local"],
											partido["marcador"],
											partido["visitante"],
											partido["fecha"].strftime("%d-%m-%Y")), partidos)) if partidos else None

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