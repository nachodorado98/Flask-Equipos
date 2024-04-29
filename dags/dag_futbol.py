from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
import os
import time

from python.src.etl import extraerData, limpiarData, cargarData
from python.src.etl_equipos import extraerDataEquipos, limpiarDataEquipos, cargarDataEquipos
from python.src.etl_info_equipo import extraerDataInfoEquipo, limpiarDataInfoEquipo, cargarDataInfoEquipo
from python.src.database.conexion import Conexion
from python.src.utils import descargar, realizarDescarga

def existe_entorno()->str:

	return "etl_ligas" if os.path.exists(os.path.join(os.getcwd(), "dags", "entorno")) else "carpeta_entorno"

def crearArchivoLog(motivo:str)->None:

	archivo_log=f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

	ruta_log=os.path.join(os.getcwd(), "dags", "entorno", "logs", archivo_log)

	with open(ruta_log, "w") as archivo:

		archivo.write(f"Error en ejecucion: {motivo}")

def ETL_Ligas()->str:

	try:
		
		data=extraerData()

		data_limpia=limpiarData(data)

		cargarData(data_limpia)

		print("ETL Ligas finalizada")

		return "etl_equipos"

	except Exception as e:

		return "log_ligas"

def ETL_Equipos()->None:

	conexion=Conexion()

	ligas=conexion.obtenerIdUrlLigas()

	for id_liga, url in ligas:

		try:

			print(f"ETL Equipos Liga Id {id_liga}")

			data=extraerDataEquipos(url)

			data_limpia=limpiarDataEquipos(data)

			cargarDataEquipos(data_limpia, id_liga)

		except Exception as e:

			crearArchivoLog(f"ETL Equipos fallido en la liga {id_liga}")		

	conexion.cerrarConexion()

	print("ETL Equipos finalizada")

	time.sleep(30)

def ETL_Info_Equipos()->None:

	conexion=Conexion()

	equipos=conexion.obtenerIdUrlEquipos()

	for id_equipo, url in equipos:

		try:

			print(f"ETL Info Equipo Id {id_equipo}")

			url_imagen=conexion.obtenerUrlImagen(id_equipo)

			data=extraerDataInfoEquipo(url)

			data_limpia=limpiarDataInfoEquipo(data)

			cargarDataInfoEquipo(data_limpia, id_equipo)

			if descargar(data_limpia, url_imagen):

				ruta_imagenes=os.path.join(os.getcwd(), "dags", "entorno", "imagenes")

				nombre_imagen=conexion.obtenerNombreEquipoUrl(id_equipo)

				print(f"Descargando imagen {nombre_imagen}...")

				realizarDescarga(data_limpia, ruta_imagenes, nombre_imagen)

		except Exception as e:

			crearArchivoLog(f"ETL Info Equipo fallido {id_equipo}")

		finally:

			time.sleep(2)

	conexion.cerrarConexion()

	print("ETL Info Equipos finalizada")


with DAG("dag_futbol",
		start_date=datetime(2024,4,10),
		description="DAG para obtener datos de la web de futbol",
		schedule_interval=None,
		catchup=False) as dag:

	tarea_existe_entorno=BranchPythonOperator(task_id="existe_entorno", python_callable=existe_entorno)

	comando_bash_entorno="cd ../../opt/airflow/dags && mkdir entorno"

	comando_bash_logs="cd ../../opt/airflow/dags/entorno && mkdir logs"

	comando_bash_imagenes="cd ../../opt/airflow/dags/entorno && mkdir imagenes"

	tarea_carpeta_entorno=BashOperator(task_id="carpeta_entorno", bash_command=comando_bash_entorno)

	tarea_carpeta_logs=BashOperator(task_id="carpeta_logs", bash_command=comando_bash_logs)

	tarea_carpeta_imagenes=BashOperator(task_id="carpeta_imagenes", bash_command=comando_bash_imagenes)

	tarea_etl_ligas=BranchPythonOperator(task_id="etl_ligas", python_callable=ETL_Ligas, trigger_rule="none_failed_min_one_success")

	tarea_log_ligas=PythonOperator(task_id="log_ligas", python_callable=crearArchivoLog, op_kwargs={"motivo": "ETL Ligas fallido"})

	tarea_etl_equipos=PythonOperator(task_id="etl_equipos", python_callable=ETL_Equipos)

	tarea_etl_info_equipos=PythonOperator(task_id="etl_info_equipos", python_callable=ETL_Info_Equipos)

tarea_existe_entorno >> [tarea_carpeta_entorno, tarea_etl_ligas]

tarea_carpeta_entorno >> tarea_carpeta_logs >> tarea_carpeta_imagenes >> tarea_etl_ligas >> [tarea_log_ligas, tarea_etl_equipos]

tarea_etl_equipos >> tarea_etl_info_equipos

