import requests
from bs4 import BeautifulSoup as bs4

from .config import URL
from .excepciones import PaginaError

class Scraper:

    def __init__(self, endpoint:str)->None:

        self.url_scrapear=URL+endpoint

    def __realizarPeticion(self)->bs4:

        peticion=requests.get(self.url_scrapear)

        if peticion.status_code!=200:

            print(f"Codigo de estado de la peticion: {peticion.status_code}")
            
            raise PaginaError("Error en la pagina")

        return bs4(peticion.text,"html.parser")