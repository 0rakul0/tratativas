import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

def gerar_dados_falsos_parquet(nome_arquivo, quantidade_linhas):
    fake = Faker()

    dados_falsos = []
    for _ in range(quantidade_linhas):
        dados = {
            'nome': fake.name(),
            'endereco': fake.address(),
            'email': fake.email(),
            'telefone': fake.phone_number(),
            'data_nascimento': fake.date_of_birth(minimum_age=18, maximum_age=65).strftime('%Y-%m-%d'),
            'profissao': fake.job(),
            # Adicione mais campos conforme necessário
        }
        dados_falsos.append(dados)

    df = pd.DataFrame(dados_falsos)
    tabela = pa.Table.from_pandas(df)
    pq.write_table(tabela, nome_arquivo, compression='snappy')

# Gerar 5 arquivos Parquet usando PyArrow
for i in range(1, 11):
    nome_arquivo = f'./parquet/dados_falsos_{i}.parquet'
    linhas = 100000
    gerar_dados_falsos_parquet(nome_arquivo, linhas)
    print(f'Arquivo {nome_arquivo} gerado com sucesso.')
