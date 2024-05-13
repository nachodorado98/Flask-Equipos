import pandas as pd
from typing import Optional

from .scraper_partidos import ScraperPartidos

from .excepciones import PartidosLimpiarError

from .database.conexion import Conexion

def extraerDataPartidos(fecha:str)->Optional[pd.DataFrame]:

	scraper=ScraperPartidos(fecha)

	partidos=scraper.obtenerPartidos()

	return partidos

def limpiarDataPartidos(tabla:pd.DataFrame)->pd.DataFrame:

	tabla["Home"]=tabla["Home"].str.replace(r"\s[a-z]{2,3}$", "", regex=True)

	tabla["Away"]=tabla["Away"].str.replace(r"^[a-z]{2,3}\s", "", regex=True)

	def limpiarPublico(cantidad:str)->int:

		try:
		
			cantidad_str=cantidad.replace(",", "")
		
			return int(cantidad_str)

		except ValueError:

			return 0

	tabla["Attendance"]=tabla["Attendance"].apply(limpiarPublico)

	def filtrarFila(codigo1:str, codigo2:str)->str:

		conexion=Conexion()

		bool1=conexion.comprobarCodigo(codigo1)

		bool2=conexion.comprobarCodigo(codigo2)

		conexion.cerrarConexion()

		return "No" if bool1 or bool2 else "Si"

	tabla["Eliminar"]=tabla.apply(lambda fila: filtrarFila(fila["CodEquipo1"], fila["CodEquipo2"]), axis=1)

	tabla_filtrada=tabla[tabla["Eliminar"]=="No"]

	if tabla_filtrada.empty:

		raise PartidosLimpiarError("No hay partidos este dia")

	tabla_columnas_seleccionadas=tabla_filtrada[["Competicion", "Round", "Fecha", "Time", "Home", "Score", "Away", "Attendance", "Venue", "CodEquipo1", "CodEquipo2"]]

	return tabla_columnas_seleccionadas.reset_index(drop=True)

def cargarDataPartidos(tabla:pd.DataFrame)->None:

	conexion=Conexion()

	partidos=tabla.values.tolist()

	conexion.insertarPartidos(partidos)

	conexion.cerrarConexion()