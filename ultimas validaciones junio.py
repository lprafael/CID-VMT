import pandas as pd
from sqlalchemy import create_engine
from datetime import date, timedelta
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib
from openpyxl import load_workbook

# Conexión a la base de datos en Microsoft Azure
azure_engine = create_engine('postgresql://vmtuser:$$Tr4nsp0rt3$$@flex-opentransit-prod-1.postgres.database.azure.com:5432/opentransit-prod')
print('Conexión a Azure establecida')

# Conexión a la base de datos en MITIC
mitic_engine = create_engine('postgresql://cid_admin_user:vmtdmtcidccm@168.90.177.232:2024/bbdd-monitoreo-cid')
print('Conexión a Mitic establecida')

# Obtener los datos relevantes de la base de datos local CIDBD
mitic_query = """
    SELECT
        g.gre_nombre,
        e.eot_nombre AS eot,
        cr.ruta_hex
    FROM
        catalogo_rutas cr
JOIN eots e ON cr.id_eot_catalogo = e.cod_catalogo
JOIN gremios g ON e.gre_id = g.gre_id
"""
mitic_df = pd.read_sql(mitic_query, mitic_engine)
print('Consulta a Mitic ejecutada')

# Realizar la primera consulta a Azure
azure_query = '''
    SELECT
        idrutaestacion,
        COUNT(*) as validaciones_24jun
    FROM
        c_transacciones
    WHERE
        tipoevento = 4
        AND idproducto in ('4d4f', '4553')
        AND fechahoraevento BETWEEN '2024-06-24 00:00:00' AND '2024-06-24 23:59:59'
    GROUP BY idrutaestacion
'''
azure_df_24jun = pd.read_sql(azure_query, azure_engine)
print('Consulta 1 de Azure realizada con éxito')

# Realizar la segunda consulta a Azure
azure_query = '''
    SELECT
        idrutaestacion,
        COUNT(*) as validaciones_29jun
    FROM
        c_transacciones
    WHERE
        tipoevento = 4
        AND idproducto in ('4d4f', '4553')
        AND fechahoraevento BETWEEN '2024-06-29 00:00:00' AND '2024-06-29 23:59:59'
    GROUP BY idrutaestacion
'''
azure_df_29jun = pd.read_sql(azure_query, azure_engine)
print('Consulta 2 de Azure realizada con éxito')

# Realizar la segunda consulta a Azure
azure_query = '''
    SELECT
        idrutaestacion,
        COUNT(*) as validaciones_29jun
    FROM
        c_transacciones
    WHERE
        tipoevento = 4
        AND idproducto in ('4d4f', '4553')
        AND fechahoraevento BETWEEN '2024-06-30 00:00:00' AND '2024-06-30 23:59:59'
    GROUP BY idrutaestacion
'''
azure_df_30jun = pd.read_sql(azure_query, azure_engine)
print('Consulta 2 de Azure realizada con éxito')

# Combinar los resultados de las dos consultas de Azure
azure_df_combined = azure_df_24jun.merge(azure_df_29jun, on='idrutaestacion', how='outer')

# Combinar los resultados de las dos consultas de Azure
azure_df_combined = azure_df_combined.merge(azure_df_30jun, on='idrutaestacion', how='outer')

# Realizar el merge con los datos de Mitic
final_df = mitic_df.merge(azure_df_combined, left_on='ruta_hex', right_on='idrutaestacion', how='outer')
print('Macheo realizado con éxito')

# Guardar los resultados en un archivo Excel
filename = 'validaciones_junio_2024.xlsx'

# Guardar directamente los datos en un nuevo archivo Excel
with pd.ExcelWriter(filename, engine='openpyxl') as writer:
    final_df.to_excel(writer, sheet_name='Validaciones', index=False, startrow=9)
print('Datos guardados en Excel con éxito')

"""
# Enviar el archivo por correo electrónico
remitente = 'billetajevmt@gmail.com'
recipient = 'transporte.mopc@gmail.com'
password = 'password'  # Cambia esto a un método seguro de obtención de la contraseña
subject = 'Reporte de Validaciones'
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