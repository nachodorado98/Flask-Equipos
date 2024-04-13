import pandas as pd
from typing import Optional, List

from .scraper_ligas import ScraperLigas

from .excepciones import LigasError, LigasExistentesError, LigaCargadaError

from .config import ENDPOINT_LIGAS, LIGAS_PAISES

from .database.conexion import Conexion

def extraerData(endpoint:str=ENDPOINT_LIGAS)->Optional[pd.DataFrame]:

	scraper=ScraperLigas(endpoint)

	ligas=scraper.obtenerLigas()

	return ligas

def limpiarData(tabla:pd.DataFrame, ligas:List=LIGAS_PAISES)->pd.DataFrame:

	tabla["Liga"]=tabla["Liga"].apply(lambda pais: pais.split("Football Clubs")[0].strip())

	tabla["CodigoPais"]=tabla["Endpoint"].apply(lambda endpoint: endpoint.split("clubs/")[1].split("/")[0])

	tabla_filtrada=tabla[tabla["Liga"].isin(ligas)]

	if tabla_filtrada.empty:

		raise LigasExistentesError("Ligas no existentes")

	return tabla_filtrada.reset_index(drop=True)

def cargarData(tabla:pd.DataFrame)->None:

	con=Conexion()

	ligas=tabla.values.tolist()

	for liga in ligas:

		if not con.existe_liga(liga[0]):

			con.insertarLiga(liga)

		else:

			raise LigaCargadaError(f"Liga {liga[0]} existente")

	con.cerrarConexion()