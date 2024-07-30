import pandas as pd
from sqlalchemy import create_engine
from datetime import date, timedelta

# Conexión a la base de datos en Microsoft Azure
azure_engine = create_engine('postgresql://vmtuser:$$Tr4nsp0rt3$$@flex-opentransit-prod-1.postgres.database.azure.com:5432/opentransit-prod')
print('Conexión a Azure establecida')

# Conexión a la base de datos en MITIC
mitic_engine = create_engine('postgresql://cid_admin_user:vmtdmtcidccm@168.90.177.232:2024/bbdd-monitoreo-cid')
print('Conexión a Mitic establecida')

# Obtener los datos relevantes de la base de datos local CIDBD
mitic_query = """
SELECT g.gre_nombre, e.eot_nombre AS eot, cr.identificador_troncal AS troncal, cr.ruta_hex
FROM catalogo_rutas cr
JOIN eots e ON cr.id_eot_catalogo = e.cod_catalogo
JOIN gremios g ON e.gre_id = g.gre_id
"""
mitic_df = pd.read_sql(mitic_query, mitic_engine)
print('Consulta a Mitic ejecutada')

# Fechas actuales y anteriores
hoy = date.today()
inicio_anio = date(hoy.year, 1, 1)
inicio_mes = date(hoy.year, hoy.month, 1)
inicio_semana = hoy - timedelta(days=hoy.weekday())

# Obtener los datos ya calculados de la tabla
datos_existentes_query = """
SELECT eot, troncal, fecha_calculo_anual, fecha_calculo_mensual, fecha_calculo_semanal 
FROM promedio_diario_operacion
"""
datos_existentes_df = pd.read_sql(datos_existentes_query, mitic_engine)
print('Consulta de datos existentes realizada con éxito')

# Determinar si es necesario recalcular los promedios
recalcular_anual = datos_existentes_df['fecha_calculo_anual'].isnull().all() or (datos_existentes_df['fecha_calculo_anual'].min() < inicio_anio)
recalcular_mensual = datos_existentes_df['fecha_calculo_mensual'].isnull().all() or (datos_existentes_df['fecha_calculo_mensual'].min() < inicio_mes)
recalcular_semanal = datos_existentes_df['fecha_calculo_semanal'].isnull().all() or (datos_existentes_df['fecha_calculo_semanal'].min() < inicio_semana)

# Obtener los datos relevantes de la base de datos en Azure
azure_query = """
WITH operaciones AS (
    SELECT 
        DISTINCT eventos_dia.idsam, 
        eventos_dia.idrutaestacion, 
        eventos_dia.fechahoraevento::date AS fecha, 
        extract(dow FROM fechahoraevento) AS dia_semana
    FROM (
        SELECT DISTINCT (c_transacciones.idsam::text || c_transacciones.consecutivoevento::text || c_transacciones.serialmediopago::text) AS evento, 
                        c_transacciones.idsam, 
                        c_transacciones.serialmediopago, 
                        c_transacciones.fechahoraevento, 
                        c_transacciones.idproducto, 
                        c_transacciones.entidad, 
                        c_transacciones.iddispositivo, 
                        c_transacciones.montoevento, 
                        c_transacciones.tipoevento, 
                        c_transacciones.idrutaestacion
        FROM c_transacciones
        WHERE c_transacciones.tipoevento IN (4, 8)
    ) eventos_dia
)
"""

# Añadir cálculos solo si es necesario
if recalcular_anual:
    azure_query += """
    ,anual_lun_vie AS (
        SELECT idrutaestacion, COUNT(DISTINCT idsam)/(SELECT COUNT(DISTINCT fechahoraevento::date) 
                                                      FROM c_transacciones 
                                                      WHERE extract(dow FROM fechahoraevento) BETWEEN 1 AND 5 
                                                      AND fechahoraevento >= date_trunc('year', current_date - interval '1 year') 
                                                      AND fechahoraevento < date_trunc('year', current_date)) AS promedio_anual_lun_vie
        FROM operaciones
        WHERE fecha >= date_trunc('year', current_date - interval '1 year')
        AND fecha < date_trunc('year', current_date)
        AND dia_semana BETWEEN 1 AND 5
        GROUP BY idrutaestacion
    ),
    anual_sab AS (
        SELECT idrutaestacion, COUNT(DISTINCT idsam)/(SELECT COUNT(DISTINCT fechahoraevento::date) 
                                                      FROM c_transacciones 
                                                      WHERE extract(dow FROM fechahoraevento) = 6 
                                                      AND fechahoraevento >= date_trunc('year', current_date - interval '1 year') 
                                                      AND fechahoraevento < date_trunc('year', current_date)) AS promedio_anual_sab
        FROM operaciones
        WHERE fecha >= date_trunc('year', current_date - interval '1 year')
        AND fecha < date_trunc('year', current_date)
        AND dia_semana = 6
        GROUP BY idrutaestacion
    ),
    anual_dom AS (
        SELECT idrutaestacion, COUNT(DISTINCT idsam)/(SELECT COUNT(DISTINCT fechahoraevento::date) 
                                                      FROM c_transacciones 
                                                      WHERE extract(dow FROM fechahoraevento) = 0 
                                                      AND fechahoraevento >= date_trunc('year', current_date - interval '1 year') 
                                                      AND fechahoraevento < date_trunc('year', current_date)) AS promedio_anual_dom
        FROM operaciones
        WHERE fecha >= date_trunc('year', current_date - interval '1 year')
        AND fecha < date_trunc('year', current_date)
        AND dia_semana = 0
        GROUP BY idrutaestacion
    )
    """
else:
    azure_query += """
    ,anual_lun_vie AS (SELECT NULL AS identidad, NULL AS idrutaestacion, NULL AS promedio_anual_lun_vie),
    anual_sab AS (SELECT NULL AS identidad, NULL AS idrutaestacion, NULL AS promedio_anual_sab),
    anual_dom AS (SELECT NULL AS identidad, NULL AS idrutaestacion, NULL AS promedio_anual_dom)
    """

if recalcular_mensual:
    azure_query += """
    ,mensual_lun_vie AS (
        SELECT idrutaestacion, COUNT(DISTINCT idsam)/(SELECT COUNT(DISTINCT fechahoraevento::date) 
                                                      FROM c_transacciones 
                                                      WHERE extract(dow FROM fechahoraevento) BETWEEN 1 AND 5 
                                                      AND fechahoraevento >= date_trunc('month', current_date - interval '1 month') 
                                                      AND fechahoraevento < date_trunc('month', current_date)) AS promedio_mensual_lun_vie
        FROM operaciones
        WHERE fecha >= date_trunc('month', current_date - interval '1 month')
        AND fecha < date_trunc('month', current_date)
        AND dia_semana BETWEEN 1 AND 5
        GROUP BY idrutaestacion
    ),
    mensual_sab AS (
        SELECT idrutaestacion, COUNT(DISTINCT idsam)/(SELECT COUNT(DISTINCT fechahoraevento::date) 
                                                      FROM c_transacciones 
                                                      WHERE extract(dow FROM fechahoraevento) = 6 
                                                      AND fechahoraevento >= date_trunc('month', current_date - interval '1 month') 
                                                      AND fechahoraevento < date_trunc('month', current_date)) AS promedio_mensual_sab
        FROM operaciones
        WHERE fecha >= date_trunc('month', current_date - interval '1 month')
        AND fecha < date_trunc('month', current_date)
        AND dia_semana = 6
        GROUP BY idrutaestacion
    ),
    mensual_dom AS (
        SELECT idrutaestacion, COUNT(DISTINCT idsam)/(SELECT COUNT(DISTINCT fechahoraevento::date) 
                                                      FROM c_transacciones 
                                                      WHERE extract(dow FROM fechahoraevento) = 0 
                                                      AND fechahoraevento >= date_trunc('month', current_date - interval '1 month') 
                                                      AND fechahoraevento < date_trunc('month', current_date)) AS promedio_mensual_dom
        FROM operaciones
        WHERE fecha >= date_trunc('month', current_date - interval '1 month')
        AND fecha < date_trunc('month', current_date)
        AND dia_semana = 0
        GROUP BY idrutaestacion
    )
    """
else:
    azure_query += """
    ,mensual_lun_vie AS (SELECT NULL AS identidad, NULL AS idrutaestacion, NULL AS promedio_mensual_lun_vie),
    mensual_sab AS (SELECT NULL AS identidad, NULL AS idrutaestacion, NULL AS promedio_mensual_sab),
    mensual_dom AS (SELECT NULL AS identidad, NULL AS idrutaestacion, NULL AS promedio_mensual_dom)
    """

if recalcular_semanal:
    azure_query += """
    ,semanal_lun_vie AS (
        SELECT idrutaestacion, COUNT(DISTINCT idsam)/(SELECT COUNT(DISTINCT fechahoraevento::date) 
                                                      FROM c_transacciones 
                                                      WHERE extract(dow FROM fechahoraevento) BETWEEN 1 AND 5 
                                                      AND fechahoraevento >= date_trunc('week', current_date - interval '1 week') 
                                                      AND fechahoraevento < date_trunc('week', current_date)) AS promedio_semanal_lun_vie
        FROM operaciones
        WHERE fecha >= date_trunc('week', current_date - interval '1 week')
        AND fecha < date_trunc('week', current_date)
        AND dia_semana BETWEEN 1 AND 5
        GROUP BY idrutaestacion
    ),
    semanal_sab AS (
        SELECT idrutaestacion, COUNT(DISTINCT idsam)/(SELECT COUNT(DISTINCT fechahoraevento::date) 
                                                      FROM c_transacciones 
                                                      WHERE extract(dow FROM fechahoraevento) = 6 
                                                      AND fechahoraevento >= date_trunc('week', current_date - interval '1 week') 
                                                      AND fechahoraevento < date_trunc('week', current_date)) AS promedio_semanal_sab
        FROM operaciones
        WHERE fecha >= date_trunc('week', current_date - interval '1 week')
        AND fecha < date_trunc('week', current_date)
        AND dia_semana = 6
        GROUP BY idrutaestacion
    ),
    semanal_dom AS (
        SELECT idrutaestacion, COUNT(DISTINCT idsam)/(SELECT COUNT(DISTINCT fechahoraevento::date) 
                                                      FROM c_transacciones 
                                                      WHERE extract(dow FROM fechahoraevento) = 0 
                                                      AND fechahoraevento >= date_trunc('week', current_date - interval '1 week') 
                                                      AND fechahoraevento < date_trunc('week', current_date)) AS promedio_semanal_dom
        FROM operaciones
        WHERE fecha >= date_trunc('week', current_date - interval '1 week')
        AND fecha < date_trunc('week', current_date)
        AND dia_semana = 0
        GROUP BY idrutaestacion
    )
    """
else:
    azure_query += """
    ,semanal_lun_vie AS (SELECT NULL AS identidad, NULL AS idrutaestacion, NULL AS promedio_semanal_lun_vie),
    semanal_sab AS (SELECT NULL AS identidad, NULL AS idrutaestacion, NULL AS promedio_semanal_sab),
    semanal_dom AS (SELECT NULL AS identidad, NULL AS idrutaestacion, NULL AS promedio_semanal_dom)
    """

azure_query += """
SELECT * FROM anual_lun_vie
UNION ALL
SELECT * FROM anual_sab
UNION ALL
SELECT * FROM anual_dom
UNION ALL
SELECT * FROM mensual_lun_vie
UNION ALL
SELECT * FROM mensual_sab
UNION ALL
SELECT * FROM mensual_dom
UNION ALL
SELECT * FROM semanal_lun_vie
UNION ALL
SELECT * FROM semanal_sab
UNION ALL
SELECT * FROM semanal_dom
"""

azure_df = pd.read_sql(azure_query, azure_engine)
print('Consulta a Azure realizada con éxito')

# Filtrar y procesar los datos necesarios
final_df = mitic_df.merge(azure_df, left_on='ruta_hex', right_on='idrutaestacion', how='right')
print('Macheo realizado con éxito')

# Proceso de respaldo y restauración en caso de errores
try:
    with mitic_engine.begin() as connection:
        # Crear una copia de seguridad de la tabla antes de insertar los nuevos datos
        connection.execute("CREATE TABLE IF NOT EXISTS promedio_diario_operacion_backup AS TABLE promedio_diario_operacion")
        
        # Insertar los nuevos datos
        final_df.to_sql('promedio_diario_operacion', connection, if_exists='replace', index=False)
        print('Datos insertados con éxito')

except Exception as e:
    print(f"Error al insertar datos en la base de datos de MITIC: {e}")
    # Si hay un error, revertir a la copia de seguridad
    rollback_query = """
    DROP TABLE IF EXISTS promedio_diario_operacion;
    ALTER TABLE promedio_diario_operacion_backup RENAME TO promedio_diario_operacion;
    """
    mitic_engine.execute(rollback_query)
    print('Copia de seguridad restaurada')

# Guardar los resultados en un archivo Excel
""" filename = 'promedio_diario_operacion.xlsx'
if os.path.exists(filename):
    book = load_workbook(filename)
    writer = pd.ExcelWriter(filename, engine='openpyxl')
    writer.book = book
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
else:
    writer = pd.ExcelWriter(filename, engine='openpyxl')

final_df.to_excel(writer, sheet_name='Operación Diaria', index=False, startrow=9)
writer.save()

# Enviar el archivo por correo electrónico
sender = 'user@example.com'
recipient = 'recipient@example.com'
password = 'password'  # Cambia esto a un método seguro de obtención de la contraseña
subject = 'Reporte de Promedio Diario de Operación'
body = 'Adjunto se encuentra el reporte del promedio diario de operación.'

msg = MIMEMultipart()
msg['From'] = sender
msg['To'] = recipient
msg['Subject'] = subject
msg.attach(MIMEText(body, 'plain'))

attachment = open(filename, 'rb')
part = MIMEBase('application', 'octet-stream')
part.set_payload(attachment.read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', 'attachment; filename= {}'.format(filename))
msg.attach(part)

server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login(sender, password)
text = msg.as_string()
server.sendmail(sender, recipient, text)
server.quit()
 """