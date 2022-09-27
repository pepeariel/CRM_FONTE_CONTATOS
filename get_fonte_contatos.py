import pandas as pd
from db_conection import create_connection
import asyncio
import aiohttp
import warnings
import os
import time
from dotenv import load_dotenv, find_dotenv
warnings.filterwarnings('ignore')

# Inciar as variáveis de ambiente
load_dotenv(find_dotenv())
SECRET_DB_PASSWORD = os.environ.get('SECRET_DB_PASSWORD')
SECRET_API_KEY = os.environ.get('SECRET_API_KEY')

class ContatoAsync:
    def __init__(self, dif) -> None:
        self.idExterno = str()
        self.cnpjCpf = str()
        self.results = []
        self.df_contatos = pd.DataFrame()
        self.idInternos = dif
        self.headers = {'token': f'{SECRET_API_KEY}',
                        'Content-Type': 'application/json'}

    def get_tasks(self, session):
        tasks = []
        for idInterno in self.idInternos:
            tasks.append(
                session.get(url = f'https://api.crmsimples.com.br/API?method=getContato&idExterno={self.idExterno}&idInterno={idInterno}&cnpjCpf={self.cnpjCpf}',
                            headers=self.headers,
                            ssl=False))
        return tasks

    async def get_fonte_contato (self):
        async with aiohttp.ClientSession() as session:
            tasks = ContatoAsync.get_tasks(self, session)
            responses = await asyncio.gather(*tasks)
            for r in responses:
                requested_id = await r.json()
                self.results.append(requested_id)

    def convert_to_df (self):
        try:
            for r in self.results:
                if 'statusCode' not in r: # Verifica se a requisição retorna um valor válido
                    df_atual = pd.json_normalize(r)
                    self.df_contatos = pd.concat([self.df_contatos, df_atual])
        except Exception as e:
            print(r)
            print('Erro devido a:', e)
        finally:
            return self.df_contatos

def GetIdDiference():
    # Tabelas necessárias para criar a lista de ids correta
    df_neg = pd.read_sql('SELECT contatoidinterno FROM crm_powerbi ', con).astype(int)
    df_contatos = pd.read_sql('SELECT contatoidinterno FROM contatos ', con).astype(int)
    df_fonte_idinterno = pd.read_sql('SELECT idinterno AS contatoidinterno FROM fonte', con).astype(int)

    # Apenas od ids que existem na tabela de negociações E contatos
    df_contatoidinterno_merged = pd.merge(left=df_neg, right=df_contatos, how='inner', on='contatoidinterno')

    # Lista com apenas os ids necessarios para requisitar
    dif = list(set(df_contatoidinterno_merged['contatoidinterno']) - set(df_fonte_idinterno['contatoidinterno']))

    return dif

def CreateLogs(msg, file):
    with open(f'{file}.txt', 'a') as f:
        now = time.strftime('%Y-%m-%d %H:%M:%S')
        print(msg)
        f.write(f'\n {msg} -- concluido: {now}')

if __name__ == '__main__':
    # Cria a conexão com o banco AWS
    engine, con = create_connection('CRM.db', SECRET_DB_PASSWORD)

    # Cria a lista de ids a serem requisitados na API
    dif = GetIdDiference()

    # Inicializa a classe de requisição assincrona
    contatos = ContatoAsync(dif)

    # Faz a requisição apenas dos novos contatos
    asyncio.run(contatos.get_fonte_contato())

    # Envio dos dados ao banco AWS
    try:
        # Converte o arquivo Json em um pandas dataframe
        df_teste = contatos.convert_to_df()

        # Renomeia as colunas corretamente para inserir no banco AWS
        df_teste = df_teste[['FonteContato','Ranking', 'Score','IdUsuarioInclusao', 'IdInterno','TipoPessoa', 'CnpjCpf']]
        df_teste.columns = [str(nome_coluna).lower() for nome_coluna in df_teste.columns]

        # Trata os valores nulos e strings onde deveriam ser floats
        df_teste = df_teste.fillna(0)
        df_teste['cnpjcpf'] = df_teste['cnpjcpf'].apply(lambda x: 0 if (x == '' or type(x)  == str()) else x)

        # Envio dos dados ao banco AWS
        df_teste.to_sql('fonte', if_exists = 'append', con=engine, index=False)

        # Cria os logs no arquivo fonte_logs.txt
        msg = f'Operacao concluida com {len(df_teste)} linhas inseridas no banco'
        CreateLogs(msg, file = 'fonte_logs')

    except Exception as e:
        msg = f'Operacao não pode ser concluida devido a: {e}'
        CreateLogs(msg, file = 'fonte_logs')

    

