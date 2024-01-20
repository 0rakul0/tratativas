import pandas as pd
from faker import Faker

def gerar_dados_falsos_csv(nome_arquivo, quantidade_linhas):
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
    df.to_csv(nome_arquivo, index=False, encoding='utf-8')

# Gerar 5 arquivos CSV usando Pandas
for i in range(1, 11):
    nome_arquivo = f'./csv/dados_falsos_{i}.csv'
    linhas = 100000
    gerar_dados_falsos_csv(nome_arquivo, linhas)
    print(f'Arquivo {nome_arquivo} gerado com sucesso.')
