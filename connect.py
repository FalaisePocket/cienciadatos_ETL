from sqlalchemy import create_engine
import yaml
import os
config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')

def connect_databases():
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
        
        # Configuración de la base de datos operativa
        config_op = config['OperationalDB']
        server = config_op['Server']
        database = config_op['Database']
        
        if config_op['Type'] == 'postgresql':
            user = config_op['User']
            password = config_op['Password']
            conn_op = f'postgresql+psycopg2://{user}:{password}@{server}:5432/{database}'
        else:
            raise Exception('No valid database type for OperationalDB')
        
        engine_op = create_engine(conn_op)
        db_op = engine_op.connect()
        
        # Configuración de la base de datos ETL
        config_etl = config['ETLDB']
        server = config_etl['Server']
        database = config_etl['Database']
        
        if config_etl['Type'] == 'postgresql':
            user = config_etl['User']
            password = config_etl['Password']
            conn_etl = f'postgresql+psycopg2://{user}:{password}@{server}:5432/{database}'
        else:
            raise Exception('No valid database type for ETLDB')
        
        engine_etl = create_engine(conn_etl)
        db_etl = engine_etl.connect()

        return db_op, db_etl

    return None
