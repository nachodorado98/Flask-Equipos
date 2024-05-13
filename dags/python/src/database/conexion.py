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

	# Metodo para obtener la informacion de las ligas
	def obtenerIdUrlLigas(self)->Optional[List[tuple]]:

		self.c.execute("""SELECT Id, Url
							FROM ligas""")

		ligas=self.c.fetchall()

		return None if not ligas else list(map(lambda liga: (liga["id"], liga["url"]), ligas))

	# Metodo para actualizar la url de la imagen de un equipo
	def actualizarUrlImagen(self, url_imagen:str, id_equipo:int)->None:

		self.c.execute("""UPDATE equipos
							SET Url_Imagen=%s
							WHERE Id=%s""",
							(url_imagen, id_equipo))

		self.confirmar()

	# Metodo para obtener el id equipo de un equipo
	def obtenerIdEquipo(self, equipo:str)->Optional[int]:

		self.c.execute("""SELECT Id
							FROM equipos
							WHERE Equipo=%s""",
							(equipo,))

		id_equipo=self.c.fetchone()

		return None if id_equipo is None else id_equipo["id"]

	# Metodo para obtener la informacion de los equipos
	def obtenerIdUrlEquipos(self)->Optional[List[tuple]]:

		self.c.execute("""SELECT Id, Url
							FROM equipos""")

		equipos=self.c.fetchall()

		return None if not equipos else list(map(lambda equipo: (equipo["id"], equipo["url"]), equipos))

	# Metodo para obtener la url imagen de un equipo por su id
	def obtenerUrlImagen(self, id_equipo:int)->Optional[str]:

		self.c.execute("""SELECT Url_Imagen
							FROM equipos
							WHERE Id=%s""",
							(id_equipo,))

		url_imagen=self.c.fetchone()

		return None if url_imagen is None else url_imagen["url_imagen"]

	# Metodo para obtener el nombre del equipo url por su id
	def obtenerNombreEquipoUrl(self, id_equipo:int)->Optional[str]:

		self.c.execute("""SELECT Equipo_Url
							FROM equipos
							WHERE Id=%s""",
							(id_equipo,))

		equipo_url=self.c.fetchone()

		return None if equipo_url is None else equipo_url["equipo_url"]

	# Metodo para comprobar que existe el codigo del equipo
	def comprobarCodigo(self, codigo:str)->bool:

		self.c.execute(f"""SELECT *
							FROM equipos
							WHERE Url LIKE '%{codigo}%'""")

		codigo=self.c.fetchone()

		return False if codigo is None else True 