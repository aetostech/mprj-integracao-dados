import os
import pathlib
import zipfile

import pandas as pd

from utils import load_zipped_input_files, standard_adjustments, convert_value_string_to_float, \
    filled_and_null_cpf_tables, verify_column_elected, compare_db_and_df_cols


class ETLDoacao:
    def __init__(self, year, raw_dir, processed_dir):
        print("Started doacao ", year)
        if year % 4 == 2:
            self.national_elections = True
        else:
            self.national_elections = False
        self.current_path = pathlib.Path(__file__).parent.absolute()
        self.raw_path = os.path.join(raw_dir, f'prestacao_de_contas_eleitorais_candidatos_{year}.zip')
        self.processed_path = os.path.join(processed_dir, 'doacao_campanha.csv')
        self.processed_dir = processed_dir
        self.path = f'receitas_candidatos_{year}' + '_{}.csv'
        self.data = pd.DataFrame()
        self.year = year
        self.db_cols = ['ano', 'documento_doador', 'nome_doador',
                        'valor_doacao', 'cpf_candidato', 'nome_candidato',
                        'cargo', 'partido', 'municipio', 'uf', 'eleito']

        self.states = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'ES', 'GO',
                       'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS',
                       'SC', 'SE', 'SP', 'TO']
        if self.national_elections:
            self.states.extend(['DF', 'BR'])

    def read_data(self):
        zf = zipfile.ZipFile(self.raw_path)
        cols = ['ANO_ELEICAO',
                'NR_CPF_CNPJ_DOADOR',
                'NM_DOADOR',
                'NM_DOADOR_RFB',
                'VR_RECEITA',
                'NR_CPF_CANDIDATO',
                'NM_CANDIDATO',
                'DS_CARGO',
                'SG_PARTIDO',
                'SG_UF',
                'NM_UE']

        self.data = load_zipped_input_files(zf, f'prestacao_contas_candidatos_{self.year}' + '_{}.csv', self.states, cols)

    def transform_data(self):
        self.data['NM_DOADOR'] = self.data.apply(lambda x: x['NM_DOADOR'] if pd.isna(x['NM_DOADOR_RFB']) or
                                                                             x['NM_DOADOR_RFB'] == ''
        else x['NM_DOADOR_RFB'], axis=1)

        self.data = self.data.rename(columns={'ANO_ELEICAO': 'ano',
                                              'NR_CPF_CNPJ_DOADOR': 'documento_doador',
                                              'NM_DOADOR': 'nome_doador',
                                              'VR_RECEITA': 'valor_doacao',
                                              'NR_CPF_CANDIDATO': 'cpf_candidato',
                                              'NM_CANDIDATO': 'nome_candidato',
                                              'DS_CARGO': 'cargo',
                                              'SG_PARTIDO': 'partido',
                                              'SG_UF': 'uf',
                                              'NM_UE': 'municipio'})

        assert (self.data[self.data['documento_doador'].str.startswith('0')].shape[0] > 0)
        self.data = standard_adjustments(self.data, self.year, self.national_elections, ['nome_candidato',
                                                                                         'nome_doador',
                                                                                         'municipio'],
                                         cpf_col='cpf_candidato')
        self.data['fonte_nome_empresa'] = ''
        self.data = convert_value_string_to_float(self.data, 'valor_doacao')
        assert (self.data[
                    (self.data['documento_doador'].isna()) |
                    (self.data['documento_doador'] == '')].shape[0] == 0)

        self.data, temp_null_cpf = filled_and_null_cpf_tables(self.data, cpf_col='cpf_candidato')

        elected_data = pd.read_csv(os.path.join(self.processed_dir, f'{self.year}_candidatura.csv'),
                                   dtype={'cpf': str},
                                   usecols=['cpf', 'ano', 'partido', 'cargo', 'eleito', 'nome'])
        elected_data['cpf'] = elected_data['cpf'].str.zfill(11)

        # A planilha de doacao nao eh agrupada. Despesa eh agrupada pelo tamanho da tabela ser muito grande

        # Funde com tabela de candidatos para obter eleitos
        self.data = (
            self.data
                .merge(elected_data
                       .rename(columns={'cpf': 'cpf_candidato'}),
                       on=['cpf_candidato', 'ano', 'partido', 'cargo'], validate='many_to_one', how='left')
                .filter(items=self.db_cols)
        )

        if temp_null_cpf.shape[0] > 0:
            # Lida com CPF nulo (portanto usa nome como substituto)
            temp_null_cpf = (
                temp_null_cpf
                    .merge(elected_data
                           .drop(columns=['cpf'])
                           .rename(columns={'nome': 'nome_candidato'}),
                           on=['nome_candidato', 'ano', 'partido', 'cargo'], validate='many_to_one', how='left')
                    .filter(items=self.db_cols)
            )

            self.data = pd.concat([self.data, temp_null_cpf])

        verify_column_elected(self.data)

        compare_db_and_df_cols(self.data, self.db_cols)

    def save_data(self):
        self.data.to_csv(self.processed_path, index=False)
