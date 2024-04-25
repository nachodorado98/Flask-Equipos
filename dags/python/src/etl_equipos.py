import pandas as pd
from typing import Optional

from .scraper_equipos import ScraperEquipos

from .excepciones import EquiposError, EquiposExistentesError

from .database.conexion import Conexion

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

def cargarDataEquipos(tabla:pd.DataFrame, id_liga:int)->None:

	con=Conexion()

	equipos=tabla.values.tolist()

	for equipo in equipos:

		if not con.existe_equipo(equipo[0]):

			equipo.append(id_liga)

			print(equipo)

			con.insertarEquipo(equipo)

	con.cerrarConexion()