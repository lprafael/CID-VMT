import pandas as pd
from sqlalchemy import create_engine
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows


# Conexión a la base de datos en Microsoft Azure
azure_engine = create_engine('postgresql://vmtuser:$$Tr4nsp0rt3$$@flex-opentransit-prod-1.postgres.database.azure.com:5432/opentransit-prod')

# Conexión a la base de datos en MITIC
mitic_engine = create_engine(f'postgresql://FPorta:portaf2024@168.90.177.232:2024/bbdd-monitoreo-cid')

# Obtener los datos relevantes de la base de datos en Azure
azure_query = """
SELECT DISTINCT eventos_dia.idsam, eventos_dia.idrutaestacion, eventos_dia.fechahoraevento::date as fecha, extract(hour FROM fechahoraevento) AS hora
FROM (SELECT DISTINCT (c_transacciones.idsam::text || c_transacciones.consecutivoevento::text || c_transacciones.serialmediopago::text) AS evento, c_transacciones.idsam, 
	  c_transacciones.serialmediopago, c_transacciones.fechahoraevento, c_transacciones.idproducto, c_transacciones.identidad, c_transacciones.iddispositivo, 
	  c_transacciones.montoevento, c_transacciones.tipoevento, c_transacciones.idrutaestacion
	 FROM c_transacciones
	  WHERE (c_transacciones.fechahoraevento BETWEEN current_date ::timestamp - interval '1 days' and current_date)
	  AND c_transacciones.TIPOEVENTO in (4,8))eventos_dia
"""
azure_df = pd.read_sql(azure_query, azure_engine)

# Obtener los datos relevantes de la base de datos MITIC filtrando por troncales
mitic_query = """
SELECT eot, ruta_dec, ruta_hex, sentido, linea, ramal, origen, destino, 
       identificacion, "identificador_troncal"
FROM catalogo_rutas
"""
mitic_df = pd.read_sql(mitic_query, mitic_engine)

# Convertir ruta_hex e idrutaestacion a cadena de texto y luego a mayúsculas
azure_df['idrutaestacion'] = azure_df['idrutaestacion'].astype(str).str.upper()
mitic_df['ruta_hex'] = mitic_df['ruta_hex'].astype(str).str.upper()

# Unir (machear) los datos basados en las columnas comunes
merged_df = pd.merge(azure_df, mitic_df, how='left', left_on='idrutaestacion', right_on='ruta_hex')


# Ordenar los datos por EOT
merged_df = merged_df.sort_values(by='eot')
#result = result.sort_values(by='eot')

# Guardar los resultados en la base de datos
merged_df.to_sql('servicios_diarios', mitic_engine, if_exists='replace', index=False)

print(merged_df)
