from flask import Blueprint, render_template, request, redirect, url_for
from datetime import datetime

from src.database.conexion import Conexion

from src.config import URL_DATALAKE

bp_detalle_partido=Blueprint("detalle_partido", __name__)

@bp_detalle_partido.route("/detalle_partido/<id_partido>", methods=["GET"])
def detalle_partido(id_partido:int):

	con=Conexion()

	if not con.partido_existe(id_partido):

		con.cerrarConexion()

		return redirect("/")

	datos_partido=con.detalle_partido(id_partido)

	fecha=datetime.strptime(datos_partido[5], "%d-%m-%Y").strftime("%Y-%m-%d")

	con.cerrarConexion()

	return render_template("detalle_partido.html",
							partido=datos_partido,
							url_imagen=URL_DATALAKE,
							fecha=fecha)
