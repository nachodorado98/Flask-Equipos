from bs4 import BeautifulSoup as bs4
import pandas as pd
from typing import Optional, List

from .scraper import Scraper

from .excepciones import LigasError

class ScraperLigas(Scraper):

    def __init__(self, endpoint:str)->None:
        super().__init__(endpoint)

    def __contenido_a_tabla(self, contenido:bs4)->bs4:

        return contenido.find("table", id="countries")

    def __obtenerColumnas(self, tabla:bs4)->List[str]:

        cabecera=tabla.find("thead").find("tr").find_all("th")

        return [columna.text for columna in cabecera]

    def __obtenerFilas(self, tabla:bs4)->List[bs4]:

        return tabla.find("tbody").find_all("tr")

    def __obtenerContenidoFilas(self, filas:List[bs4])->List[tuple]:

        filas_primera_columna=[fila.find("th") for fila in filas]

        return [(fila.text, fila.find("a", href=True)["href"]) for fila in filas_primera_columna]

    def __obtenerDataLimpia(self, tabla:bs4)->pd.DataFrame:

        columnas=self.__obtenerColumnas(tabla)

        filas=self.__obtenerFilas(tabla)

        contenido_filas=self.__obtenerContenidoFilas(filas)

        return pd.DataFrame(contenido_filas, columns=["Liga", "Endpoint"])

    def obtenerLigas(self)->pd.DataFrame:

        try:

            contenido=self._Scraper__realizarPeticion()

            tabla=self.__contenido_a_tabla(contenido)

            return self.__obtenerDataLimpia(tabla)

        except Exception:

            raise LigasError("Error en obtener las ligas")