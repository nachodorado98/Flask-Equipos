from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import time
import os

from python.src.etl_partidos import extraerDataPartidos, limpiarDataPartidos, cargarDataPartidos
from python.src.excepciones import PartidosLimpiarError
from python.src.utils import fechas_etl, etl_partidos_disponibles

def crearArchivoLog(motivo:str)->None:

	archivo_log=f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

	ruta_log=os.path.join(os.getcwd(), "dags", "entorno", "logs", archivo_log)

	with open(ruta_log, "w") as archivo:

		archivo.write(f"Error en ejecucion: {motivo}")

def ETL(fecha:str)->None:

	print(f"Realizando ETL {fecha}")

	try:
		
		data=extraerDataPartidos(fecha)

		data_limpia=limpiarDataPartidos(data)

		cargarDataPartidos(data_limpia)

	except PartidosLimpiarError:

		print("No hay partidos disponibles para los equipos")

	except Exception:

		crearArchivoLog(f"ETL Partidos fallido dia {fecha}")

def ETL_Partidos()->None:

	if not etl_partidos_disponibles():

		raise Exception("No se pueden extraer los partidos porque no hay equipos ni ligas")

	fechas=fechas_etl("2024-05-01")

	if fechas:

		print(f"-----------ETL Partidos desde {fechas[0]} hasta {fechas[-1]}-----------")

		for fecha in fechas:

			ETL(fecha)
			
			time.sleep(2)

		print("ETL Partidos Finalizado")

	else:

		print("ETL Datos Actualizados")


with DAG("dag_partidos",
		start_date=datetime(2024,5,13),
		description="DAG para obtener datos de los partidos de la web de futbol",
		schedule_interval=timedelta(days=1),
		catchup=False) as dag:
		
	tarea_etl_partidos=PythonOperator(task_id="etl_partidos", python_callable=ETL_Partidos)

tarea_etl_partidos