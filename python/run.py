import time

from src.etl import extraerData, limpiarData, cargarData

def pipeline()->None:

	try:

		data=extraerData()

		data_limpia=limpiarData(data)

		print(data_limpia)

		cargarData(data_limpia)

		print("Pipeline finalizado")

	except AttributeError as e:

		print("Reconectando en 5 segundos...")

		time.sleep(5)

		pipeline()

pipeline()