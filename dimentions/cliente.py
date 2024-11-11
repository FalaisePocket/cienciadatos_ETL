import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from connect import connect_databases


db_op, db_etl = connect_databases()

# Load the required tables into pandas DataFrames
cliente = pd.read_sql_query('SELECT * FROM public.cliente', db_op)

clientelimpio=cliente[['cliente_id', 'nit_cliente', 'nombre','ciudad_id']]

ciudad=pd.read_sql_query('SELECT * FROM public.ciudad', db_op)

ciudadlimpia=ciudad[['ciudad_id','nombre']]


# Fusionar las tablas clientelimpio y ciudadlimpia usando la columna ciudad_id
cliente_ciudad = clientelimpio.merge(ciudadlimpia, on='ciudad_id', how='left', suffixes=('', '_ciudad'))





########################################
###Transform



##retirar 'email' 'direccion' 'telefono' 'tipo_cliente' 'activo' 'coordinador_id' 'sector'

# Select the desired columns

cliente_ciudad = cliente_ciudad.drop(columns='ciudad_id')

print(cliente_ciudad.head())


#####################Load####################################################
#######################################################################



cliente_ciudad.to_sql('DimCliente', db_etl, if_exists='replace', index=False)

