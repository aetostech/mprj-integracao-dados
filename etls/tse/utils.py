import os
import pathlib
import sys

import pandas as pd
from tqdm import tqdm


def load_zipped_input_files(zip_file, file_name_format, states, cols):
    data = pd.DataFrame()
    read_type = {'NR_CPF_CNPJ_DOADOR': str, 'NR_CPF_CANDIDATO': str, 'NR_CPF_CNPJ_FORNECEDOR': str}
    for uf in tqdm(states, leave=False, desc='reading_data'):
        new_data = pd.read_csv(zip_file.open(os.path.join(file_name_format.format(uf))),
                               sep=';', encoding='latin1', usecols=cols, dtype=read_type)  # , nrows=5)
        data = pd.concat([data, new_data], sort=False, ignore_index=True)
    return data


def basic_assertions(df, year):
    # Valida se ano esta correto
    assert (df[df['ano'] != year].shape[0] == 0)
    assert (df[df['ano'] == year].shape[0] == df.shape[0])


def party_name_conversion(df):
    print("Converting party names...")
    conversion_dict = {'PATRIOTA': 'PATRI'}
    df['partido'] = df['partido'].apply(lambda x: conversion_dict[x] if x in list(conversion_dict) else x)
    return df


def adjust_cpf(df, cpf_col='cpf'):
    # Codigo -4 (encontrado no formato '000000000-4') eh usado para informacao nao divulgavel. Deve ser NULL
    df[cpf_col] = df[cpf_col].str.zfill(11)
    df[cpf_col] = df[cpf_col].apply(lambda x: None if x == '000000000-4' else x)
    print("NULL CPFs: ", df[df[cpf_col].isna()].shape[0])
    return df


def compare_db_and_df_cols(df, db_cols, check_order=True):
    df_cols = list(df.columns)
    try:
        assert (len(df_cols) == len(db_cols))
    except AssertionError:
        print("Number of columns is not the same")
        print('Number of columns db:', len(db_cols))
        print('Number of columns local:', len(df_cols))
        print('List of local columns:', list(df.columns))
        print('List of db columns:', db_cols)
        raise AssertionError
    if check_order:
        for i in range(0, len(df_cols)):
            try:
                if check_order:
                    assert (df_cols[i] == db_cols[i])
                else:
                    assert (df_cols[i] in db_cols)
            except AssertionError:
                print('Database and local data columns do not match')
                print('Column db:', db_cols[i])
                print('Column local:', db_cols[i])
                print('List of local columns:', list(df.columns))
                print('List of db columns:', db_cols)
                raise AssertionError


def dob_to_int(dob):
    if type(dob) == str:
        split_dob = dob.split('/')
        return int(split_dob[2] + split_dob[1] + split_dob[0])
    else:
        return None


def dob_conversion(df):
    """Converte a data de nascimento no formato DD/MM/AAAA para AAAAMMDD
    """
    print('Converting date of birth...')

    df['data_nascimento'] = df['data_nascimento'].apply(dob_to_int)
    return df


def verify_column_elected(df):
    # Verifica se eleito foi obtido corretamente apos merge
    try:
        assert (df[df['eleito'].isna()].shape[0] == 0)
    except AssertionError:
        print()
        print("Rows without match with elections result table: ", df[df['eleito'].isna()].shape[0])
        print("Examples")
        print(df[df['eleito'].isna()].sample(5).filter(
            items=['cpf_candidato', 'uf', 'cargo', 'partido']))
    try:
        assert (True in df['eleito'].tolist())
    except AssertionError:
        print("No elected candidates found")
        print("Size of table: ", df.shape[0])
        raise AssertionError
    try:
        assert (False in df['eleito'].tolist())
    except AssertionError:
        print("No unelected candidates found")
        print("Size of table: ", df.shape[0])
        print("Examples: (check if they were indees elected)")
        print(df.sample(5).filter(
            items=['cpf_candidato', 'uf', 'cargo', 'partido', 'eleito']))
        raise AssertionError


def standard_adjustments(df, year, national_elections, name_adjust_cols, cpf_col='cpf'):
    basic_assertions(df, year)
    df = party_name_conversion(df)
    try:
        df = dob_conversion(df)
    except KeyError:
        print("Data frame does not contain date of birth column. Skipping")
        pass
    for col in name_adjust_cols:
        print(f"Adjusting string names from column {col}...")
        df[col] = (df[col].str.upper().str.strip().
                   str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8'))
    df['cargo'] = df['cargo'].apply(lambda x: x.upper())
    if national_elections:
        df['municipio'] = ''
    df = adjust_cpf(df, cpf_col=cpf_col)
    return df



def filled_and_null_cpf_tables(df, cpf_col='cpf'):
    temp_null_cpf = df[(df[cpf_col].isna()) | (df[cpf_col] == '')]
    df = df[(~df[cpf_col].isna()) & (df[cpf_col].isna() != '')]
    return df, temp_null_cpf


def convert_value_string_to_float(df, col):
    df[col] = df[col].apply(lambda x: float(x.replace(',', '.')))
    return df


def truncate_string_cols(df, col_list, size):
    for col in col_list:
        df[col] = df[col].apply(lambda x: x.strip() if not pd.isna(x) else x)
        df[col] = df[col].apply(lambda x: x[0:size - 2] + '..'
        if not pd.isna(x) and type(x) == str and len(x) > size else x)
    return df


def wide_adjust_cpf(df, cpf_col):
    df[cpf_col] = df[cpf_col].apply(lambda x: x.replace('.', '').replace('/', '').replace('-', '').strip() if
    type(x) == str else x)
    return df
