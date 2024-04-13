from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

from python.src.etl import extraerData, limpiarData, cargarData
from python.src.excepciones import LigaCargadaError

def extraccion(**kwarg)->None:

	try:

		data=extraerData()

	except Exception:

		raise Exception("Error en la extraccion")

	kwarg["ti"].xcom_push(key="data", value=data)

def transformacion(**kwarg)->None:

	data=kwarg["ti"].xcom_pull(key="data", task_ids="extraccion")

	try:

		data_limpia=limpiarData(data)

	except Exception:

		raise Exception("Error en la transformacion")

	kwarg["ti"].xcom_push(key="data_limpia", value=data_limpia)

def carga(**kwarg)->None:

	data_limpia=kwarg["ti"].xcom_pull(key="data_limpia", task_ids="transformacion")

	try:

		cargarData(data_limpia)

		print("Carga finalizada")

	except LigaCargadaError as e:

		print(e)


with DAG("dag_ligas",
		start_date=datetime(2024,4,10),
		description="DAG para obtener datos de la web de ligas de futbol",
		schedule_interval=None,
		catchup=False) as dag:

	tarea_extraccion=PythonOperator(task_id="extraccion", python_callable=extraccion)

	tarea_transformacion=PythonOperator(task_id="transformacion", python_callable=transformacion)

	tarea_carga=PythonOperator(task_id="carga", python_callable=carga)

tarea_extraccion >> tarea_transformacion >> tarea_carga