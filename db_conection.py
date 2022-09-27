import sqlalchemy as sa

def create_connection(db_file, SECRET_DB_PASSOWRD):
    try:
        host = 'database-1.c7xrnorluyxo.us-east-1.rds.amazonaws.com'
        port = '5432'
        user = 'postgres'
        password = SECRET_DB_PASSOWRD
        engine = sa.create_engine(f'postgresql+psycopg2://{user}:{password}@{host}')
        con = engine.connect()
        print('Conexão com o banco AWS realizado com sucesso!')

    except Exception as erro:
        print('Erro ao conectar devido a:', erro)
    
    return engine, con