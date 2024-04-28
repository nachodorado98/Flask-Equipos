from typing import Optional

from .scraper_info_equipo import ScraperInfoEquipo

def extraerDataInfoEquipo(endpoint:str)->Optional[str]:

	scraper=ScraperInfoEquipo(endpoint)

	url_escudo=scraper.obtenerInfo()

	return url_escudo