import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from connect import connect_databases


db_op, db_etl = connect_databases()

# Load the required tables into pandas DataFrames
sede = pd.read_sql_query('SELECT * FROM public.sede', db_op)
#ciudad = pd.read_sql_query('SELECT * FROM public.ciudad', db_op)



########################################
###Transform
#id, nombre, ciudad_id
#ciudad_id, nombre

dimsede=sede[['sede_id','nombre']]
'''ciudad=ciudad[['ciudad_id','nombre']]
ciudad=ciudad.rename(columns={'nombre': 'ciudad'})
dimsede=sede.merge(
    ciudad,
    left_on='ciudad_id',
    right_on='ciudad_id',
    how='left'
)

dimsede=dimsede.drop(columns='ciudad_id')'''
#####################Load####################################################
#######################################################################



dimsede.to_sql('DimSede', db_etl, if_exists='replace', index=False)

