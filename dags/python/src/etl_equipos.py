import pandas as pd
from typing import Optional

from .scraper_equipos import ScraperEquipos

from .excepciones import EquiposError, EquiposExistentesError

def extraerDataEquipos(endpoint:str)->Optional[pd.DataFrame]:

	scraper=ScraperEquipos(endpoint)

	equipos=scraper.obtenerEquipos()

	return equipos

def limpiarDataEquipos(tabla:pd.DataFrame)->pd.DataFrame:

	tabla["Nombre_Endpoint"]=tabla["Endpoint"].apply(lambda endpoint: endpoint.split("history/")[1].split("-Stats")[0].lower())

	tabla_masculino=tabla[tabla["Gender"]=="M"]

	tabla_primera_segunda=tabla_masculino[~tabla_masculino["Comp"].isin([""])]

	if tabla_primera_segunda.empty:

		raise EquiposExistentesError("Equipos no existentes")

	tabla_filtrada=tabla_primera_segunda[["Squad", "Endpoint", "Nombre_Endpoint"]]

	return tabla_filtrada.reset_index(drop=True)