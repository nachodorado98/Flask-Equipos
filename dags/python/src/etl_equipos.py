import pandas as pd
from typing import Optional

from .scraper_equipos import ScraperEquipos

from .excepciones import EquiposError

def extraerDataEquipos(endpoint:str)->Optional[pd.DataFrame]:

	scraper=ScraperEquipos(endpoint)

	equipos=scraper.obtenerEquipos()

	return equipos