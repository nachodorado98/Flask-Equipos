from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
import os

from python.src.etl import extraerData, limpiarData, cargarData

def existe_carpeta()->str:

	return "etl_ligas" if os.path.exists(os.path.join(os.getcwd(), "dags", "logs")) else "carpeta_logs"

def ETL_Ligas()->str:

	try:
		data=extraerData()

		data_limpia=limpiarData(data)

		cargarData(data_limpia)

		print("ETL Ligas finalizada")

		return "etl_equipos"

	except Exception as e:

		return "log_ligas"

def crearArchivoLog(motivo:str)->None:

	archivo_log=f"log_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"

	ruta_log=os.path.join(os.getcwd(), "dags", "logs", archivo_log)

	with open(ruta_log, "w") as archivo:

		archivo.write(f"Error en ejecucion: {motivo}")

def ETL_Equipos()->str:

	return "ETL Equipos"


with DAG("dag_futbol",
		start_date=datetime(2024,4,10),
		description="DAG para obtener datos de la web de futbol",
		schedule_interval=None,
		catchup=False) as dag:

	tarea_existe_carpeta=BranchPythonOperator(task_id="existe_carpeta", python_callable=existe_carpeta)

	comando_bash="cd ../../opt/airflow/dags && mkdir logs"

	tarea_carpeta_logs=BashOperator(task_id="carpeta_logs", bash_command=comando_bash)

	tarea_etl_ligas=BranchPythonOperator(task_id="etl_ligas", python_callable=ETL_Ligas, trigger_rule="none_failed_min_one_success")

	tarea_log_ligas=PythonOperator(task_id="log_ligas", python_callable=crearArchivoLog, op_kwargs={"motivo": "ETL Ligas fallido"})

	tarea_etl_equipos=PythonOperator(task_id="etl_equipos", python_callable=ETL_Equipos)

tarea_existe_carpeta >> [tarea_carpeta_logs, tarea_etl_ligas]

tarea_carpeta_logs >> tarea_etl_ligas >> [tarea_log_ligas, tarea_etl_equipos]