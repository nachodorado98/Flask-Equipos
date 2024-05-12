from bs4 import BeautifulSoup as bs4
import pandas as pd
from typing import Optional, List
from datetime import datetime, date

from .scraper import Scraper

from .config import ENDPOINT_PARTIDOS

from .excepciones import ErrorFechaFormato, ErrorFechaPosterior, PaginaError, PartidosExtraidosError

class ScraperPartidos(Scraper):

	def __init__(self, fecha:str)->None:

		self.fecha=self.__comprobarFecha(fecha)

		super().__init__(ENDPOINT_PARTIDOS+self.fecha)

	def __comprobarFecha(self, fecha:str)->str:

		try:

			fecha_datetime=datetime.strptime(fecha, "%Y-%m-%d").date()

		except ValueError:

			raise ErrorFechaFormato("Error de formato de fecha. El formato debe ser yyyy-mm-dd")

		if fecha_datetime>date.today():

			 raise ErrorFechaPosterior("La fecha no puede ser posterior al día de hoy")

		return fecha

	def obtenerFecha(self)->str:

		return self.fecha

	def __contenido_a_tablas(self, contenido:bs4)->List[bs4]:

		return contenido.find_all("div", class_="table_wrapper tabbed")

	def __obtenerTitulo(self, tabla:bs4)->str:
	
		return tabla.find("span").text.strip(r'">')

	def __obtenerTituloTabla(self, tabla:bs4)->str:
		
		return tabla.find("table").find("a").text

	def __obtenerColumnas(self, tabla:bs4)->List[str]:
	
		cabecera=tabla.find("table").find("thead")
		
		columnas=cabecera.find("tr").find_all("th")
		
		return [columna.text for columna in columnas]

	def __obtenerContenidoFilas(self, tabla:bs4)->List[List]:

		filas=tabla.find("table").find("tbody").find_all("tr")

		def limpiarFila(fila:bs4)->List[str]:
	
			head=fila.find("th").text
				
			fila_contenido=fila.find_all("td")

			def obtenerCodigoNone(fila)->Optional[str]:

				fila_dividida=fila.split("/en/squads/")

				if len(fila_dividida)!=2:

					return None

				return fila_dividida[1].split("/")[0]

			codigos_nones=[obtenerCodigoNone(fila.find("a").get("href")) for fila in fila_contenido if fila.find("a")]

			codigos_equipos=list(filter(lambda codigo: codigo is not None, codigos_nones))

			fila_completa=[head]+[valor.text for valor in fila_contenido]+codigos_equipos

			return fila_completa
		
		return [limpiarFila(fila) for fila in filas]  

	def __limpiarTabla(self, tabla:bs4)->pd.DataFrame:
	
		titulo=self.__obtenerTitulo(tabla)
		
		titulo_tabla=self.__obtenerTituloTabla(tabla)
		
		assert titulo==titulo_tabla

		columnas=self.__obtenerColumnas(tabla)

		contenido_filas=self.__obtenerContenidoFilas(tabla)
		
		df=pd.DataFrame(contenido_filas, columns=columnas+["CodEquipo1", "CodEquipo2"])
		
		df["Competicion"]=titulo
		
		if "Sem." not in list(df.columns):
			
			df["Sem."]="-"

		df["Fecha"]=self.fecha
		
		return df[["Competicion", "Round", "Fecha", "Time", "Home", "Score", "Away", "Attendance", "Venue", "CodEquipo1", "CodEquipo2"]]

	def __obtenerDataLimpia(self, tablas:List[bs4])->Optional[pd.DataFrame]:

		tablas_limpias=[self.__limpiarTabla(tabla) for tabla in tablas]

		if not tablas_limpias:

			raise PartidosExtraidosError("No hay partidos disponibles para extraer")

		return pd.concat(tablas_limpias).reset_index(drop=True)

	def obtenerPartidos(self)->pd.DataFrame:

		contenido=self._Scraper__realizarPeticion()

		tablas=self.__contenido_a_tablas(contenido)

		return self.__obtenerDataLimpia(tablas)