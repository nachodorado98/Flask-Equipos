from src.etl import extraerData, limpiarData

data=extraerData()

data_limpia=limpiarData(data)

print(data_limpia)