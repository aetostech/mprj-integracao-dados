import os
import zipfile

import pandas as pd

from utils import load_zipped_input_files, convert_value_string_to_float, filled_and_null_cpf_tables, \
    verify_column_elected, standard_adjustments, compare_db_and_df_cols


class ETLDespesa:
    def __init__(self, year, raw_dir, processed_dir):
        print("Started despesa ", year)
        if year % 4 == 2:
            self.national_elections = True
        else:
            self.national_elections = False
        self.raw_path = os.path.join(raw_dir, f'prestacao_de_contas_eleitorais_candidatos_{year}.zip')
        self.processed_dir = processed_dir
        self.processed_path = os.path.join(processed_dir, 'despesa_campanha.csv')
        self.data = pd.DataFrame()
        self.year = year
        self.db_cols = ['ano', 'uf', 'municipio', 'documento_fornecedor', 'cpf_candidato',
                        'numero_candidato', 'partido', 'cargo', 'valor_despesa', 'eleito']

        self.states = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'ES', 'GO',
                       'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR',
                       'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']
        if self.national_elections:
            self.states.extend(['DF', 'BR'])

    def read_data(self):
        zf = zipfile.ZipFile(self.raw_path)
        cols = ['ANO_ELEICAO', 'SG_UF', 'NM_UE', 'NR_CPF_CNPJ_FORNECEDOR',
                'NR_CPF_CANDIDATO', 'NR_CANDIDATO', 'SG_PARTIDO', 'DS_CARGO', 'VR_DESPESA_CONTRATADA', 'NM_CANDIDATO']

        self.data = load_zipped_input_files(zf, f'despesas_contratadas_candidatos_{self.year}' + '_{}.csv', self.states,
                                            cols)

    def transform_data(self):
        self.data = self.data.rename(columns={'ANO_ELEICAO': 'ano',
                                              'SG_UF': 'uf',
                                              'NM_UE': 'municipio',
                                              'NR_CPF_CNPJ_FORNECEDOR': 'documento_fornecedor',
                                              'NR_CPF_CANDIDATO': 'cpf_candidato',
                                              'NR_CANDIDATO': 'numero_candidato',
                                              'SG_PARTIDO': 'partido',
                                              'DS_CARGO': 'cargo',
                                              'VR_DESPESA_CONTRATADA': 'valor_despesa',
                                              'NM_CANDIDATO': 'nome_candidato'})

        self.data = standard_adjustments(self.data, self.year, self.national_elections, ['municipio',
                                                                                         'nome_candidato'],
                                         cpf_col='cpf_candidato')
        assert (self.data[self.data['documento_fornecedor'].str.startswith('0')].shape[0] > 0)
        self.data = convert_value_string_to_float(self.data, 'valor_despesa')

        print("Verifying that all documents from suppliers are filled...")
        assert (self.data[
                    (self.data['documento_fornecedor'].isna()) |
                    (self.data['documento_fornecedor'] == '')].shape[0] == 0)

        self.data, temp_null_cpf = filled_and_null_cpf_tables(self.data, cpf_col='cpf_candidato')

        # Agrupa dados para obter somatorio de despesas. Despesa eh agrupada pelo tamanho da tabela ser muito grande
        self.data = (
            self.data
                .groupby(['cpf_candidato', 'ano', 'partido', 'cargo', 'documento_fornecedor'])
                .agg({'uf': 'max', 'numero_candidato': 'max', 'valor_despesa': 'sum', 'nome_candidato': 'max',
                      'municipio': 'max'})
                .reset_index()
                .filter(items=self.db_cols)
        )

        print("Reading candidates data...")
        elected_data = pd.read_csv(os.path.join(self.processed_dir, f'{self.year}_candidatura.csv'),
                                   dtype={'cpf': str},
                                   usecols=['cpf', 'ano', 'partido', 'cargo', 'eleito', 'nome'])
        print("Filling CPFs with 0 in candidates data...")
        elected_data['cpf'] = elected_data['cpf'].str.zfill(11)

        # Funde com tabela de candidatos para obter eleitos
        print("Merging tables... This can take a while")
        self.data = (
            self.data
                .merge(elected_data
                       .drop(columns=['nome'])
                       .rename(columns={'cpf': 'cpf_candidato'}),
                       on=['cpf_candidato', 'ano', 'partido', 'cargo'], how='left')
                .filter(items=self.db_cols)
        )

        if temp_null_cpf.shape[0] > 0:
            # Lida com CPF nulo (portanto usa nome como substituto)
            temp_null_cpf = (
                temp_null_cpf
                    .groupby(['nome_candidato', 'ano', 'partido', 'cargo', 'documento_fornecedor'])
                    .agg({'uf': 'max', 'numero_candidato': 'max', 'valor_despesa': 'sum', 'cpf_candidato': 'max',
                          'municipio': 'max'})
                    .reset_index()
                    .filter(items=self.db_cols)
            )

            print("Merging more tables... This can take a while")
            temp_null_cpf = (
                temp_null_cpf
                    .merge(elected_data
                           .drop(columns=['cpf']),
                           on=['nome_candidato', 'ano', 'partido', 'cargo'], validate='many_to_one', how='left')
                    .filter(items=self.db_cols)
            )

            self.data = pd.concat([self.data, temp_null_cpf])

        verify_column_elected(self.data)
        compare_db_and_df_cols(self.data, self.db_cols)

    def save_data(self):
        self.data.to_csv(self.processed_path, index=False)
