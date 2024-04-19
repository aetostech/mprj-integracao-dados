import os
import pandas as pd
import datetime

df = []
for fname in os.listdir('.'):
    if fname.endswith('.csv'):
        tmp = pd.read_csv(fname, sep=';', encoding='iso-8859-1')
        df.append(tmp)

df = pd.concat(df)
df['documento'] = df['CPF/CNPJ'].apply(lambda x: ''.join([c for c in x if c.isnumeric()]) if not pd.isna(x) else x)
df = df.rename(columns={'UF': 'uf',
                        'Município': 'municipio',
                        'Nome': 'nome',
                        'Processo': 'processo',
                        'Trânsito em julgado': 'data_transito_julgado'})
df = df.drop(['Ficha', 'CPF/CNPJ', 'Deliberações'], axis=1)
df['data_transito_julgado'] = pd.to_datetime(df['data_transito_julgado'], format='%d/%m/%Y')
df['data_observacao'] = datetime.date.today()
df.to_csv('contas_irregulares.csv', sep='|')
