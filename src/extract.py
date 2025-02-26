import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os 

### Variáveis 
load_dotenv()
commodities = ['AAPL', 'MSFT', 'META', 'TSLA', 'OPEN', 'NVDA', 'GOOG']
DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

### Função para obter dados
def get_data(symbol, period='1d', interval='1d'):
    ticker = yf.Ticker(symbol) 
    dados = ticker.history(period=period, interval=interval)[['Close']]
    dados['symbol'] = symbol 
    return dados

### Função para obter todos os dados
def get_all_data(commodities):
    all_data = []

    for symbol in commodities:
        dados = get_data(symbol)
        all_data.append(dados)
    
    return pd.concat(all_data, ignore_index=True) 

### Função para salvar dados no DW
def save_data_dw(df, schema='public'):
    df.to_sql('commodities', engine, if_exists='replace', index=True, index_label='Date', schema=schema)


# Executando a função
dados_concatenados = get_all_data(commodities)
save_data_dw(dados_concatenados, schema='public')
