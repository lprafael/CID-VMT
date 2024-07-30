#Para exportar los datos del archivo Catálogo de Rutas a una base de datos
import pandas as pd
from sqlalchemy import create_engine

# Ruta del archivo Excel
excel_file_path = 'C:/Users/support/Documents/VMT - CID/Catálogo de Rutas v 29-04-2024.xlsx'

# Leer el archivo Excel
df = pd.read_excel(excel_file_path)

# Convertir el DataFrame a un formato adecuado para PostgreSQL (opcional)
df.columns = df.columns.str.lower()  # Convertir los nombres de las columnas a minúsculas para evitar problemas con las mayúsculas en PostgreSQL

# Conectar a la base de datos PostgreSQL
engine = create_engine('postgresql://cid_admin_user:vmtdmtcidccm@168.90.177.232:2024/bbdd-monitoreo-cid')

# Limpiar los datos para manejar la codificación
df = df.applymap(lambda x: x.encode('utf-8', errors='ignore').decode('utf-8') if isinstance(x, str) else x)

# Importar los datos a PostgreSQL
try:
    df.to_sql('catalogo_rutas', engine, if_exists='replace', index=False)
    print("Datos importados correctamente a la tabla catalogo_rutas.")
except UnicodeDecodeError as e:
    print("Error de codificación:", e)
except Exception as e:
    print("Ha ocurrido un error:", e)
