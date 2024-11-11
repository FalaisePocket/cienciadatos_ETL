import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from connect import connect_databases




db_op, db_etl = connect_databases()



#Extract


###mensajeria_estado???
##mensajeria_estadoservicio
##mensajeria_tiposervicio????
##mensajeria_servicio


mensajeria_estado =  pd.read_sql_query('SELECT * FROM public.mensajeria_estado', db_op)
mensajeria_estadosservicio =  pd.read_sql_query('SELECT * FROM public.mensajeria_estadosservicio', db_op)
mensajeria_tiposervicio =  pd.read_sql_query('SELECT * FROM public.mensajeria_tiposervicio', db_op)
mensajeria_servicio =  pd.read_sql_query('SELECT * FROM public.mensajeria_servicio', db_op)


dimciudad =  pd.read_sql_query('SELECT * FROM public."DimCiudad"', db_etl)
dimcliente =  pd.read_sql_query('SELECT * FROM public."DimCliente"', db_etl)
dimsede =  pd.read_sql_query('SELECT * FROM public."DimSede"', db_etl)



###hecho    
mensajeria_estadosservicio=mensajeria_estadosservicio[['id'
                                                       ,'fecha','hora',
                                                       'estado_id','servicio_id'
                                                       ]]
mensajeria_servicio=mensajeria_servicio[['id','cliente_id','mensajero_id',
                                         'tipo_servicio_id']]
mensajeria_tiposervicio=mensajeria_tiposervicio[['id', 'nombre',]]
mensajeria_estado=mensajeria_estado[['id','nombre']]


####mensajeria_estado
mensajeria_tiposervicio = mensajeria_tiposervicio.rename(columns={'id': 'tipo_servicio_id','nombre': 'tipo_servicio'})
mensajeria_servicio= mensajeria_servicio.rename(columns={'id': 'servicio_id'})
mensajeria_estadosservicio=mensajeria_estadosservicio.rename(columns={'id':'estadosservicios_id'})
mensajeria_estado=mensajeria_estado.rename(columns={'id':'estado_id','nombre': 'estado_nombre'})


facttable=mensajeria_estadosservicio.merge(
    mensajeria_servicio,
    left_on='servicio_id',
    right_on='servicio_id',
    how='left')

facttable=facttable.merge(
    mensajeria_tiposervicio,
    left_on='tipo_servicio_id',
    right_on='tipo_servicio_id',
    how='left'   
)

facttable=facttable.merge(
    mensajeria_estado,
    left_on='estado_id',
    right_on='estado_id',
    how='left'
)



facttable=facttable.rename(columns={'estadosservicios_id':'id'})
facttable=facttable[['id','servicio_id','mensajero_id',
                     'tipo_servicio','estado_nombre',
                     'fecha','hora']]

print(facttable.head())




















'''

completados = facttable[facttable['estado_id'] == 6]

# Agrupar por fecha y contar los servicios completados por día
entregas_por_dia = completados.groupby('fecha').size()
#.reset_index(name='cantidad_entregas')

# Crear la tabla de hechos
tabla_hechos = entregas_por_dia.rename(columns={'fecha': 'fecha_id'})

# Crear la dimensión de fecha
dimension_fecha = entregas_por_dia[['fecha']].drop_duplicates().reset_index(drop=True)
dimension_fecha['fecha_id'] = dimension_fecha.index + 1  # Crear un ID único para cada fecha

# Unir fecha_id en la tabla de hechos
tabla_hechos = tabla_hechos.merge(dimension_fecha, left_on='fecha_id', right_on='fecha', how='left')
tabla_hechos = tabla_hechos[['fecha_id', 'cantidad_entregas']]

'''