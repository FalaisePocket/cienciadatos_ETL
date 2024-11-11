import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from connect import connect_databases

db_op, db_etl = connect_databases()

dim_ciudad = pd.read_sql_query('SELECT * FROM public.ciudad', db_op)

#####################Transform###########################################
#########################################################################

# Drop columns
dim_ciudad = dim_ciudad.drop(columns='departamento_id')

# Rename Columns



#############################Load######################################
#######################################################################
dim_ciudad.to_sql('DimCiudad', db_etl, if_exists='replace',index=False)
