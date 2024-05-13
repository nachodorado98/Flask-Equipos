from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

from python.src.etl_partidos import extraerDataPartidos, limpiarDataPartidos, cargarDataPartidos

def ETL_Partidos()->None:

	try:
		
		data=extraerDataPartidos("2024-05-13")

		data_limpia=limpiarDataPartidos(data)

		cargarDataPartidos(data_limpia)

		print("ETL Partidos finalizada")

	except Exception:

		raise Exception("Error partidos")

with DAG("dag_partidos",
		start_date=datetime(2024,5,13),
		description="DAG para obtener datos de los partidos de la web de futbol",
		schedule_interval=None,
		catchup=False) as dag:
		
	tarea_etl_partidos=PythonOperator(task_id="etl_partidos", python_callable=ETL_Partidos)

tarea_etl_partidos