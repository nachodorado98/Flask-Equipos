import pandas as pd
from typing import Optional

from .scraper_ligas import ScraperLigas
from .excepciones import LigasError
from .config import ENDPOINT_LIGAS

def extraerData(endpoint:str=ENDPOINT_LIGAS)->Optional[pd.DataFrame]:

	try:

		scraper=ScraperLigas(endpoint)

		return scraper.obtenerLigas()

	except LigasError:

		print("La ligas no estan disponibles")