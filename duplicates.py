import ssl
from db_conection import create_connection
import pandas as pd
import requests
import sqlalchemy as sa

SECRET_API_KEY = "711ad004c12-c61b-42ef-b95a-770c00fd5863"
SECRET_DB_PASSOWRD = "3xpdOcTa0HlIjG"
headers = {'token': f'{SECRET_API_KEY}',
            'Content-Type': 'application/json'}

idExterno = str()
cnpjCpf = str()

df_final = pd.DataFrame()

def get_contatos_sem_estado():
        df_sem_estado = pd.read_sql('SELECT * FROM contatos', con)
        df_sem_estado = df_sem_estado[df_sem_estado['cidade'] == '']
        df_sem_estado = df_sem_estado.drop_duplicates(subset='contatoidinterno')
        
        for contato in df_sem_estado['contatoidinterno']:
            request = requests.get(f'https://api.crmsimples.com.br/API?method=getContato&idExterno={idExterno}&idInterno={contato}&cnpjCpf={cnpjCpf}',
                                    headers=headers)
            response = request.json()['ListEndereco']
            df_atual = pd.json_normalize(response)
            df_atual['contatoidinterno'] = contato
            df_final = pd.concat([df_final, df_atual])

        try:
            print(df_final)

        except Exception as e:
            print('erro devido a',e)

if __name__ == '__main__':

    # Cria a conexão com o banco de dados
    engine, con = create_connection('CRM.db', SECRET_DB_PASSOWRD)

    # Cria a classe sqlalchemy para executar querys no banco
    metadata = sa.MetaData(bind=None)
    contatos = sa.Table('contatos', 
                        metadata, 
                        autoload=True, 
                        autoload_with=engine)
    
    # Lê a tabela com os ids (contatoidinterno) com os estados corretos para atualizar o banco
    df_com_estado = pd.read_excel(r'C:/Users/Allservice/Downloads/contatos_com_estado.xlsx')
    df_com_estado = df_com_estado.dropna()

    # Atualiza a tabela de contatos no banco com os estados corretos
    for contatoidinterno1, cidade1 in zip(df_com_estado['contatoidinterno'],df_com_estado['Uf']):
        
        #stmt = sa.update(contatos).values({'cidade':f'{cidade1}'}).where(contatos.c.contatoidinterno == contatoidinterno1)
        
        print(f'Contato {contatoidinterno1} atualizado para {cidade1}')
    
    # Requisição dos ids duplicados
    #ids = df_fonte['idinterno']
    #df_duplicados = df_fonte[ids.isin(ids[ids.duplicated()])].sort_values('idinterno')
    #print(df_duplicados)





