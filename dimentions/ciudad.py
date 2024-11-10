import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from connect import connect_databases

db_op, db_etl = connect_databases()

dim_currency = pd.read_sql_query('SELECT * FROM public.ciudad', db_op)

#####################Transform###########################################
#########################################################################

# Drop columns
dim_currency = dim_currency.drop(columns='departamento_id')

# Rename Columns



#############################Load######################################
#######################################################################
dim_currency.to_sql('DimCiudad', db_etl, if_exists='replace',index=False)
