from bs4 import BeautifulSoup as bs4

from .scraper import Scraper

from .excepciones import InfoEquipoError

class ScraperInfoEquipo(Scraper):

	def __init__(self, endpoint:str)->None:
		super().__init__(endpoint)

	def __contenido_info(self, contenido:bs4)->bs4:

		return contenido.find("div", id="info")

	def __obtenerUrlEscudo(self, info:bs4)->str:

		return info.find("div", class_="media-item logo loader").find("img")["src"]

	def obtenerInfo(self)->str:

		try:

			contenido=self._Scraper__realizarPeticion()

			info=self.__contenido_info(contenido)

			return self.__obtenerUrlEscudo(info)

		except Exception:

			raise InfoEquipoError("Error en obtener la info del equipo")