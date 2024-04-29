from typing import Optional

from .scraper_info_equipo import ScraperInfoEquipo

from .excepciones import UrlImagenExistenteError

from .database.conexion import Conexion

def extraerDataInfoEquipo(endpoint:str)->Optional[str]:

	scraper=ScraperInfoEquipo(endpoint)

	url_escudo=scraper.obtenerInfo()

	return url_escudo

def limpiarDataInfoEquipo(url_imagen:str)->Optional[str]:

	condicion_longitud=len(url_imagen)<1
	condicion_inicio=not url_imagen.startswith("https://cdn.ssref.net/")
	condicion_fin=not url_imagen.endswith(".png")

	if any([condicion_longitud, condicion_inicio, condicion_fin]):

		raise UrlImagenExistenteError("Error en la url de la imagen")

	return url_imagen

def cargarDataInfoEquipo(url_imagen:str, id_equipo:int)->None:

	con=Conexion()

	con.actualizarUrlImagen(url_imagen, id_equipo)

	con.cerrarConexion()