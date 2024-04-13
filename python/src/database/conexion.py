import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List

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
							VALUES(%s,%s,%s)""",
							tuple(liga))

		self.confirmar()

	# Metodo para saber si existe la liga
	def existe_liga(self, liga:str)->bool:

		self.c.execute("""SELECT *
							FROM ligas
							WHERE Liga=%s""",
							(liga,))

		return False if self.c.fetchone() is None else True