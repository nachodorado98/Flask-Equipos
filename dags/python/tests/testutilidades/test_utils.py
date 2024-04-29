import pytest
import os

from src.utils import descargar, realizarDescarga
from src.excepciones import DescargaImagenError

@pytest.mark.parametrize(["nueva", "antigua"],
	[
		(None, "url_antigua"),
		(None, None),
		("url_antigua", "url_antigua"),
		("url_nueva", "url_nueva")
	]
)
def test_descargar_no(nueva, antigua):

	assert descargar(nueva, antigua) is False

@pytest.mark.parametrize(["nueva", "antigua"],
	[
		("url_antigua", None),
		("url_nueva", "url_antigua")
	]
)
def test_descargar_si(nueva, antigua):

	assert descargar(nueva, antigua)

@pytest.mark.parametrize(["url"],
	[(None,), ("url_antigua",), ("url_nueva",)]
)
def test_realizar_descarga_error(url):

	with pytest.raises(DescargaImagenError):

		realizarDescarga(url, "ruta", "nombre")

def borrarCarpeta(ruta:str)->None:

	if os.path.exists(ruta):

		os.rmdir(ruta)

def crearCarpeta(ruta:str)->None:

	if not os.path.exists(ruta):

		os.mkdir(ruta)

def vaciarCarpeta(ruta:str)->None:

	if os.path.exists(ruta):

		for archivo in os.listdir(ruta):

			os.remove(os.path.join(ruta, archivo))

def test_realizar_descarga():

	ruta_carpeta=os.path.join(os.getcwd(), "testutilidades", "Imagenes_Tests")

	crearCarpeta(ruta_carpeta)

	vaciarCarpeta(ruta_carpeta)

	url_imagen="https://cdn.ssref.net/req/202404172/tlogo/fb/db3b9613.png"

	realizarDescarga(url_imagen, ruta_carpeta, "atleti")

	assert os.path.exists(os.path.join(ruta_carpeta, "atleti.png"))

	vaciarCarpeta(ruta_carpeta)

	borrarCarpeta(ruta_carpeta)