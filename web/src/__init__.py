from flask import Flask

from .blueprints.partidos_fecha import bp_partidos_fecha
from .blueprints.detalle_partido import bp_detalle_partido

# Funcion para crear la instancia de la aplicacion
def crear_app(configuracion:object)->Flask:

	app=Flask(__name__, template_folder="templates")

	app.config.from_object(configuracion)

	app.register_blueprint(bp_partidos_fecha)
	app.register_blueprint(bp_detalle_partido)

	return app