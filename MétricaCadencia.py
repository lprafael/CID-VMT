import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Conexión a la base de datos
azure_engine = create_engine('postgresql://vmtuser:$$Tr4nsp0rt3$$@flex-opentransit-prod-1.postgres.database.azure.com:5432/opentransit-prod')
mitic_engine = create_engine('postgresql://cid_admin_user:vmtdmtcidccm@168.90.177.232:2024/bbdd-monitoreo-cid')

# Definir los puntos de interés
puntos_interes = [
    {"nombre": "4 Mojones", "lat": -25.339700, "lon": -57.584852}, 
    {"nombre": "San Lorenzo", "lat": -25.342924, "lon": -57.506273}
]

# Rango de proximidad en grados
rango_proximidad = 0.001

# Definir fechas de análisis
fecha_inicio = '2024-07-22 05:00:00'
fecha_fin = '2024-07-22 07:59:59'

# Consultar datos de rutas y EOT
mitic_query = """
SELECT e.eot_nombre AS eot, cr.ruta_hex
FROM catalogo_rutas cr
JOIN eots e ON cr.id_eot_catalogo = e.eot_id
"""
mitic_df = pd.read_sql(mitic_query, mitic_engine)

# Función para obtener transacciones cercanas a un punto de interés
def obtener_transacciones_cercanas(lat, lon, rango, engine):
    query = f"""
    SELECT idSam, idrutaestacion, fechahoraevento, latitude, longitude
    FROM c_transacciones
    WHERE latitude BETWEEN {lat - rango} AND {lat + rango}
      AND longitude BETWEEN {lon - rango} AND {lon + rango}
      AND tipoevento in (4, 8)
      AND fechahoraevento BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
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

# Convertir fechas
transacciones_df['fechahoraevento'] = pd.to_datetime(transacciones_df['fechahoraevento'])

# Ordenar datos
transacciones_df = transacciones_df.sort_values(by=['idsam', 'fechahoraevento'])

# Identificar cambios de ruta
transacciones_df['prev_idrutaestacion'] = transacciones_df.groupby('idsam')['idrutaestacion'].shift(1)
transacciones_df['cambio_ruta'] = transacciones_df['idrutaestacion'] != transacciones_df['prev_idrutaestacion']

# Convertir 'cambio_ruta' a 1 (True) o 0 (False)
transacciones_df['cambio_ruta'] = transacciones_df['cambio_ruta'].astype(int)

# Crear serie_id como acumulado de 'cambio_ruta'
transacciones_df['serie_id'] = transacciones_df.groupby('idsam')['cambio_ruta'].cumsum()

# Obtener la primera transacción por idSam en cada serie
first_transactions_df = transacciones_df.groupby(['idsam', 'idrutaestacion', 'serie_id']).first().reset_index()

# Ordenar por ruta, idSam, serie_id y fecha
first_transactions_df = first_transactions_df.sort_values(by=['idrutaestacion', 'idsam', 'serie_id', 'fechahoraevento'])

# Calcular diferencias de tiempo entre transacciones en segundos
first_transactions_df['cadencia'] = first_transactions_df.groupby(['idrutaestacion', 'serie_id'])['fechahoraevento'].diff().dt.total_seconds()
first_transactions_df['cadencia_mins'] = first_transactions_df['cadencia'] / 60

# Filtrar resultados válidos
cadencias_df = first_transactions_df.dropna(subset=['cadencia_mins'])

# Promedio de cadencia por EOT y idrutaestacion
promedio_cadencia_df = cadencias_df.groupby(['eot', 'idrutaestacion'])['cadencia_mins'].mean().reset_index()

# Guardar resultados
filename = 'promedio_cadencia_por_EOT_y_idrutaestacion.xlsx'
promedio_cadencia_df.to_excel(filename, index=False)
print(f'Resultados guardados en {filename}')
