from bs4 import BeautifulSoup as bs4
import pandas as pd
from typing import Optional, List

from .scraper import Scraper

from .excepciones import EquiposError

class ScraperEquipos(Scraper):

	def __init__(self, endpoint:str)->None:
		super().__init__(endpoint)

	def __contenido_a_tabla(self, contenido:bs4)->bs4:

		return contenido.find("table", id="clubs")

	def __obtenerColumnas(self, tabla:bs4)->List[str]:

		cabecera=tabla.find("thead").find("tr").find_all("th")

		return [columna.text for columna in cabecera]+["Endpoint"]

	def __obtenerFilas(self, tabla:bs4)->List[bs4]:

		return tabla.find("tbody").find_all("tr")

	def __obtenerContenidoFilas(self, filas:List[bs4])->List[List]:

		def limpiarFila(fila)->List[str]:
	
			fila_primera_celda=fila.find("th")
			
			nombre, url=contenido=fila_primera_celda.text, fila_primera_celda.find("a", href=True)["href"]
			
			resto_filas=[celda.text for celda in fila.find_all("td")]
			
			return [nombre]+resto_filas+[url]

		return list(map(limpiarFila, filas))

	def __obtenerDataLimpia(self, tabla:bs4)->pd.DataFrame:

		columnas=self.__obtenerColumnas(tabla)

		filas=self.__obtenerFilas(tabla)

		contenido_filas=self.__obtenerContenidoFilas(filas)

		return pd.DataFrame(contenido_filas, columns=columnas)

	def obtenerEquipos(self)->pd.DataFrame:

		try:

			contenido=self._Scraper__realizarPeticion()

			tabla=self.__contenido_a_tabla(contenido)

			return self.__obtenerDataLimpia(tabla)

		except Exception:

			raise EquiposError("Error en obtener los equipos")