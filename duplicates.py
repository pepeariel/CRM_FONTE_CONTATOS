from db_conection import create_connection
import pandas as pd

SECRET_DB_PASSOWRD = "3xpdOcTa0HlIjG"
SECRET_API_KEY = "711ad004c12-c61b-42ef-b95a-770c00fd5863"

engine, con = create_connection('CRM.db', SECRET_DB_PASSOWRD)

# Requisição dos ids duplicados
#ids = df_fonte['idinterno']
#df_duplicados = df_fonte[ids.isin(ids[ids.duplicated()])].sort_values('idinterno')





