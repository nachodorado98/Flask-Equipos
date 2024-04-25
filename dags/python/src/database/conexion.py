import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Optional

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

	#Metodo para insertar una liga
	def insertarLiga(self, liga:List[str])->None:

		self.c.execute("""INSERT INTO ligas (liga, url, siglas)
							VALUES(%s, %s, %s)""",
							tuple(liga))

		self.confirmar()

	# Metodo para saber si existe la liga
	def existe_liga(self, liga:str)->bool:

		self.c.execute("""SELECT *
							FROM ligas
							WHERE Liga=%s""",
							(liga,))

		return False if self.c.fetchone() is None else True

	# Metodo para obtener el id liga de una liga
	def obtenerIdLiga(self, liga:str)->Optional[int]:

		self.c.execute("""SELECT Id
							FROM ligas
							WHERE Liga=%s""",
							(liga,))

		id_liga=self.c.fetchone()

		return None if id_liga is None else id_liga["id"]

	# Metodo para saber si existe el equipo
	def existe_equipo(self, equipo:str)->bool:

		self.c.execute("""SELECT *
							FROM equipos
							WHERE Equipo=%s""",
							(equipo,))

		return False if self.c.fetchone() is None else True

	#Metodo para insertar un equipo
	def insertarEquipo(self, equipo:List[str])->None:

		self.c.execute("""INSERT INTO equipos (equipo, url, equipo_url, id_liga)
							VALUES(%s, %s, %s, %s)""",
							tuple(equipo))

		self.confirmar()