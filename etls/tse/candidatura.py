import os
import zipfile

import pandas as pd

from utils import load_zipped_input_files, filled_and_null_cpf_tables, standard_adjustments, compare_db_and_df_cols


class ETLCandidatura:
    def __init__(self, year, raw_dir, processed_dir):
        print("Started candidatura ", year)
        if year % 4 == 2:
            self.national_elections = True
        else:
            self.national_elections = False
        self.raw_path = os.path.join(raw_dir, f'consulta_cand_{year}.zip')
        self.processed_path = os.path.join(processed_dir, f'{year}_candidatura.csv')
        self.data = pd.DataFrame()
        self.year = year
        self.db_cols = ['cpf', 'titulo_eleitor', 'nome', 'uf', 'municipio',
                        'numero', 'partido', 'cargo', 'ano', 'eleito']

        self.states = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'ES', 'GO', 'MA',
                       'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ',
                       'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']

        if self.national_elections:
            self.states.extend(['DF', 'BR'])

    def read_data(self):
        zf = zipfile.ZipFile(os.path.join(self.raw_path))
        cols = ['ANO_ELEICAO', 'SG_UF', 'NM_UE', 'NM_CANDIDATO',
                'NR_CPF_CANDIDATO', 'SG_PARTIDO', 'DT_NASCIMENTO',
                'NR_TITULO_ELEITORAL_CANDIDATO', 'CD_SIT_TOT_TURNO', 'DS_CARGO', 'NR_CANDIDATO']

        self.data = load_zipped_input_files(zf, f'consulta_cand_{self.year}' + '_{}.csv', self.states, cols)

    def transform_data(self):
        self.data = self.data.rename(columns={'NR_CPF_CANDIDATO': 'cpf',
                                              'NR_TITULO_ELEITORAL_CANDIDATO': 'titulo_eleitor',
                                              'NM_CANDIDATO': 'nome',
                                              'ANO_ELEICAO': 'ano',
                                              'DT_NASCIMENTO': 'data_nascimento',
                                              'SG_UF': 'uf',
                                              'NM_UE': 'municipio',
                                              'NR_CANDIDATO': 'numero',
                                              'SG_PARTIDO': 'partido',
                                              'DS_CARGO': 'cargo',
                                              'CD_SIT_TOT_TURNO': 'eleito'})

        # Atribui eleito aos codigos 1, 2 ou 3
        self.data['eleito'] = self.data['eleito'].apply(lambda x: True if x == 1 or x == 2 or x == 3 else False)

        self.data = standard_adjustments(self.data, self.year, self.national_elections, ['nome', 'municipio'])

        self.data, temp_null_cpf = filled_and_null_cpf_tables(self.data, cpf_col='cpf')

        # Unicidade de uma linha eh dada por 'cpf', 'ano', 'partido', 'cargo'
        self.data = (
            self.data
                .groupby(['cpf', 'ano', 'partido', 'cargo'])
                .agg({'titulo_eleitor': 'max', 'nome': 'max', 'data_nascimento': 'max', 'uf': 'max', 'eleito': 'max',
                      'municipio': 'max', 'numero': 'max'})
                .reset_index()
                .filter(items=self.db_cols)
        )

        temp_null_cpf = (
            temp_null_cpf
                .groupby(['nome', 'ano', 'partido', 'cargo'])
                .agg({'titulo_eleitor': 'max', 'cpf': 'max', 'data_nascimento': 'max', 'uf': 'max', 'eleito': 'max',
                      'municipio': 'max', 'numero': 'max'})
                .reset_index()
                .filter(items=self.db_cols)
        )

        self.data = pd.concat([self.data, temp_null_cpf])

        compare_db_and_df_cols(self.data, self.db_cols)

    def save_data(self):
        self.data.to_csv(self.processed_path, index=False)
