import pandas as pd
from typing import Optional

from .scraper_partidos import ScraperPartidos

def extraerDataPartidos(fecha:str)->Optional[pd.DataFrame]:

	scraper=ScraperPartidos(fecha)

	partidos=scraper.obtenerPartidos()

	return partidos