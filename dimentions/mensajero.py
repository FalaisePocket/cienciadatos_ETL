##Mensajeroaquitoy
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from connect import connect_databases

db_op, db_etl = connect_databases()

cliente_mensajero = pd.read_sql_query('SELECT * FROM public.clientes_mensajeroaquitoy', db_op)
users=pd.read_sql_query('SELECT * FROM public.auth_user', db_op)


#####################Transform###########################################
#########################################################################
cliente_mensajero=cliente_mensajero[['id','user_id']]
users=users[['id','username','first_name','last_name']]
users=users.rename(columns={'id': 'user_id'})


dim_mensajero=cliente_mensajero.merge(
    users,
    left_on='user_id',
    right_on='user_id',
    how='left'
)

dim_mensajero = dim_mensajero.drop(columns='user_id')


#############################Load######################################
#######################################################################
dim_mensajero.to_sql('DimMensajero', db_etl, if_exists='replace',index=False)
