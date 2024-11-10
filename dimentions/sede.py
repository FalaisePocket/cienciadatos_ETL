import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from connect import connect_databases


db_op, db_etl = connect_databases()

# Load the required tables into pandas DataFrames
sede = pd.read_sql_query('SELECT * FROM public.sede', db_op)



########################################
###Transform



sedelimpio=sede[['sede_id', 'nombre']]




#####################Load####################################################
#######################################################################



sedelimpio.to_sql('DimSede', db_etl, if_exists='replace', index=False)

