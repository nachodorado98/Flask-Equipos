from datetime import datetime, timedelta

# Funcion para comprobar si la fecha tiene el formato correcto
def fecha_formato_correcto(fecha:str)->bool:

	try:

		fecha_formato=datetime.strptime(fecha, "%Y-%m-%d")

		return True

	except ValueError:

		return False

# Funcion para obtener la fecha anterior a una fecha
def fecha_anterior(fecha:str)->str:

	fecha_datetime=datetime.strptime(fecha, "%Y-%m-%d")

	fecha_anterior=fecha_datetime-timedelta(days=1)

	return fecha_anterior.strftime("%Y-%m-%d")

# Funcion para obtener la fecha siguiente a una fecha
def fecha_siguiente(fecha:str)->str:

	fecha_datetime=datetime.strptime(fecha, "%Y-%m-%d")

	fecha_anterior=fecha_datetime+timedelta(days=1)

	return fecha_anterior.strftime("%Y-%m-%d")

# Funcion para obtener la fecha en una cadena mas bonita
def fecha_bonita(fecha:str)->str:

    fecha_datetime=datetime.strptime(fecha, "%Y-%m-%d")
    
    dias_semana=["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]

    meses=["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    
    dia_semana=dias_semana[fecha_datetime.weekday()]

    nombre_mes=meses[fecha_datetime.month]

    return f"{dia_semana} {fecha_datetime.day} de {nombre_mes} de {fecha_datetime.year}"

# Funcion para comprobar si es maxima la fecha
def es_maxima(fecha:str, fecha_maxima:str)->bool:

	return True if fecha==fecha_maxima else False

# Funcion para comprobar si es minima la fecha
def es_minima(fecha:str, fecha_minima:str)->bool:

	return True if fecha==fecha_minima else False

# Funcion para saber si la fecha esta disponible (esta entre la minima y la maxima)
def fecha_disponible(fecha:str, fecha_minima:str, fecha_maxima:str)->bool:

	if es_maxima(fecha, fecha_maxima) or es_minima(fecha, fecha_minima):

		return True

	bool_maxima=datetime.strptime(fecha, "%Y-%m-%d")>datetime.strptime(fecha_maxima, "%Y-%m-%d")

	bool_minima=datetime.strptime(fecha, "%Y-%m-%d")<datetime.strptime(fecha_minima, "%Y-%m-%d")

	return False if bool_maxima or bool_minima else True