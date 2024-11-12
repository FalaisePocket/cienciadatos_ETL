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

print(facttable.columns)

facttable=facttable.drop(columns='id')

facttable=facttable.pivot(index='servicio_id', columns='estado_nombre')




facttable.to_sql('FactEstados', db_etl, if_exists='replace', index=False)

