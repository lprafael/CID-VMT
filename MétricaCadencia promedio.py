import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Conexión a la base de datos en Microsoft Azure
azure_engine = create_engine('postgresql://vmtuser:$$Tr4nsp0rt3$$@flex-opentransit-prod-1.postgres.database.azure.com:5432/opentransit-prod')
print('Conexión a Azure establecida')

# Conexión a la base de datos en MITIC
mitic_engine = create_engine('postgresql://cid_admin_user:vmtdmtcidccm@168.90.177.232:2024/bbdd-monitoreo-cid')
print('Conexión a Mitic establecida')

# Definir los puntos de interés
puntos_interes = [
    {"nombre": "4 Mojones", "lat": -25.339700, "lon": -57.584852}, 
    {"nombre": "San Lorenzo", "lat": -25.342924, "lon": -57.506273}
]

# Rango de proximidad en grados (ejemplo: 0.001 grados)
rango_proximidad = 0.001

# Obtener los datos relevantes de la base de datos local CIDBD
mitic_query = """
SELECT e.eot_nombre AS eot, cr.ruta_hex
FROM catalogo_rutas cr
JOIN eots e ON cr.id_eot_catalogo = e.eot_id
"""
mitic_df = pd.read_sql(mitic_query, mitic_engine)
print('Consulta a Mitic ejecutada')

# Función para obtener las transacciones cercanas a un punto de interés
def obtener_transacciones_cercanas(lat, lon, rango, engine):
    query = f"""
    SELECT idSam,
        idrutaestacion,
        fechahoraevento,
        latitude,
        longitude
    FROM
        c_transacciones
    WHERE 
        latitude BETWEEN {lat - rango} AND {lat + rango} AND
        longitude BETWEEN {lon - rango} AND {lon + rango} AND
        tipoevento in (4, 8) AND
        fechahoraevento BETWEEN '2024-07-22 05:00:00' AND '2024-07-22 07:59:59'
    """
    return pd.read_sql(query, engine)

# Obtener transacciones para cada punto de interés
transacciones = []
for punto in puntos_interes:
    lat = punto["lat"]
    lon = punto["lon"]
    transacciones_df = obtener_transacciones_cercanas(lat, lon, rango_proximidad, azure_engine)
    transacciones_df['punto'] = punto['nombre']
    transacciones.append(transacciones_df)

# Concatenar todas las transacciones
transacciones_df = pd.concat(transacciones)
print('Transacciones obtenidas')

# Machear transacciones con datos de MITIC
transacciones_df = transacciones_df.merge(mitic_df, left_on='idrutaestacion', right_on='ruta_hex', how='left')
print('Macheo realizado')

# Convertir 'fechahoraevento' a datetime
transacciones_df['fechahoraevento'] = pd.to_datetime(transacciones_df['fechahoraevento'])

# Ordenar por ruta, idSam y fecha
transacciones_df = transacciones_df.sort_values(by=['idrutaestacion', 'idsam', 'fechahoraevento'])

# Obtener la primera transacción por idSam en cada ruta
first_transactions_df = transacciones_df.groupby(['idrutaestacion', 'idsam']).first().reset_index()

# Ordenar por ruta y fecha
first_transactions_df = first_transactions_df.sort_values(by=['idrutaestacion', 'fechahoraevento'])

# Calcular diferencias de tiempo entre buses en segundos
first_transactions_df['cadencia'] = first_transactions_df.groupby('idrutaestacion')['fechahoraevento'].diff().dt.total_seconds()

# Convertir cadencia de segundos a minutos
first_transactions_df['cadencia_mins'] = first_transactions_df['cadencia'] / 60

# Filtrar resultados donde la cadencia es no nula
cadencias_df = first_transactions_df.dropna(subset=['cadencia_mins'])

# Reindexar el DataFrame para evitar problemas en el cálculo de promedio
cadencias_df = cadencias_df.reset_index(drop=True)

# Calcular el promedio de la cadencia por EOT y idrutaestacion
promedio_cadencia_df = cadencias_df.groupby(['eot', 'idrutaestacion'])['cadencia_mins'].mean().reset_index()

# Guardar los resultados en un archivo Excel
filename = 'promedio_cadencia_por_EOT_y_idrutaestacion.xlsx'
promedio_cadencia_df.to_excel(filename, index=False)
print(f'Resultados guardados en {filename}')
