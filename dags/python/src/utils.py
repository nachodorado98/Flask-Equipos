import wget
import os

from .excepciones import DescargaImagenError

# Funcion para verificar si hay que descargar o no la imagen
def descargar(url_nueva:str, url_antigua:str)->bool:

	return False if url_nueva is None or url_nueva==url_antigua else True

# Funcion para realizar la descarga de la imagen
def realizarDescarga(url_imagen:str, ruta_imagenes:str, nombre:str)->None:

	try:
		
		wget.download(url_imagen, os.path.join(ruta_imagenes, f"{nombre}.png"))
	
	except Exception as e:
	
		raise DescargaImagenError(f"No se ha podido descargar la imagen de {nombre}")