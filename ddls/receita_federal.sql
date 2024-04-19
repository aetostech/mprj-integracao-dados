CREATE DATABASE receita_federal;

-- receita_federal.contador definition

CREATE TABLE receita_federal.contador
(
    `num_registro_crc` Int32,
    `num_telefone` Int32,
    `num_ddd` Int32,
    `num_cep` Int32,
    `ind_matriz_filial` Int32,
    `ind_tipo` String,

    `tipo_logradouro` String,

    `cod_tipo_logradouro` String,

    `sigla_uf_crc` String,

    `sigla_uf` String,

    `num_cnpj_empresa` String,

    `descr_email` String,

    `nome_municipio` String,

    `nome_bairro` String,

    `descr_complemento_logradouro` String,

    `num_logradouro` String,

    `descr_logradouro` String,

    `nome` String,

    `num_cpf` String,

    `num_cnpj` String
)
ENGINE = MergeTree
ORDER BY num_cnpj_empresa
SETTINGS index_granularity = 8192;


-- receita_federal.pessoa_fisica definition

CREATE TABLE receita_federal.pessoa_fisica
(

    `pessoafisicaid` Int32,

    `cpf` String,

    `nome` String,

    `situacaocadastral` String,

    `residenteexterior` String,

    `codigopaisexterior` String,

    `nomepaisexterior` String,

    `nomemae` String,

    `datanascimento` Int32,

    `sexo` Int32,

    `naturezaocupacao` Int32,

    `ocupacaoprincipal` Int32,

    `exercicioocupacao` Int32,

    `tipologradouro` String,

    `logradouro` String,

    `numerologradouro` String,

    `complemento` String,

    `bairro` String,

    `cep` String,

    `uf` String,

    `codigomunicipio` String,

    `municipio` String,

    `ddd` String,

    `telefone` String,

    `unidadeadministrativa` String,

    `anoobito` Float32,

    `estrangeiro` String,

    `dataatualizacao` String,

    `tituloeleitor` String,

    `cpfmascarado` String
)
ENGINE = MergeTree
PRIMARY KEY cpf
ORDER BY cpf
SETTINGS index_granularity = 8192;

-- receita_federal.raw_pessoa_juridica definition

CREATE TABLE receita_federal.raw_pessoa_juridica
(

    `cnpj` String,

    `dados` String
)
ENGINE = MergeTree
PRIMARY KEY cnpj
ORDER BY cnpj
SETTINGS index_granularity = 8192;


CREATE materialized VIEW receita_federal.pessoa_juridica ENGINE = MergeTree() PRIMARY KEY cnpj AS
(SELECT
    LPAD(
        cnpj,
        14,
        '0'
    ) AS cnpj,
    simpleJSONExtractRaw(dados, 'bairro')::VARCHAR AS bairro,
    simpleJSONExtractRaw(dados, 'capital_social')::FLOAT AS capital_social,
    simpleJSONExtractRaw(dados, 'cep')::VARCHAR AS cep,
    simpleJSONExtractRaw(dados, 'cnae_fiscal')::VARCHAR AS cnae_fiscal,
    simpleJSONExtractRaw(dados, 'cnae_fiscal_descricao')::VARCHAR AS cnae_fiscal_descricao,
    simpleJSONExtractRaw(dados, 'cnaes_secundarios') AS cnaes_secundarios,
    simpleJSONExtractRaw(dados, 'codigo_municipio')::VARCHAR AS codigo_municipio,
    simpleJSONExtractRaw(dados, 'codigo_municipio_ibge')::VARCHAR AS codigo_municipio_ibge,
    simpleJSONExtractRaw(dados, 'codigo_natureza_juridica')::VARCHAR AS codigo_natureza_juridica,
    simpleJSONExtractRaw(dados, 'codigo_pais')::VARCHAR AS codigo_pais,
    simpleJSONExtractRaw(dados, 'codigo_porte')::VARCHAR AS codigo_porte,
    simpleJSONExtractRaw(dados, 'complemento')::VARCHAR AS complemento,
    simpleJSONExtractRaw(dados, 'data_exclusao_do_mei')::DATE AS data_exclusao_do_mei,
    simpleJSONExtractRaw(dados, 'data_exclusao_do_simples')::DATE AS data_exclusao_do_simples,
    simpleJSONExtractRaw(dados, 'data_inicio_atividade')::DATE AS data_inicio_atividade,
    simpleJSONExtractRaw(dados, 'data_opcao_pelo_mei')::DATE AS data_opcao_pelo_mei,
    simpleJSONExtractRaw(dados, 'data_opcao_pelo_simples')::DATE AS data_opcao_pelo_simples,
    simpleJSONExtractRaw(dados, 'data_situacao_cadastral')::DATE AS data_situacao_cadastral,
    simpleJSONExtractRaw(dados, 'data_situacao_especial')::DATE AS data_situacao_especial,
    simpleJSONExtractRaw(dados, 'ddd_fax')::VARCHAR AS ddd_fax,
    simpleJSONExtractRaw(dados, 'ddd_telefone_1')::VARCHAR AS ddd_telefone_1,
    simpleJSONExtractRaw(dados, 'ddd_telefone_2')::VARCHAR AS ddd_telefone_2,
    simpleJSONExtractRaw(dados, 'descricao_identificador_matriz_filial')::VARCHAR AS descricao_identificador_matriz_filial,
    simpleJSONExtractRaw(dados, 'descricao_motivo_situacao_cadastral')::VARCHAR AS descricao_motivo_situacao_cadastral,
    simpleJSONExtractRaw(dados, 'descricao_porte')::VARCHAR AS descricao_porte,
    simpleJSONExtractRaw(dados, 'descricao_situacao_cadastral')::VARCHAR AS descricao_situacao_cadastral,
    simpleJSONExtractRaw(dados, 'descricao_tipo_de_logradouro')::VARCHAR AS descricao_tipo_de_logradouro,
    simpleJSONExtractRaw(dados, 'email')::VARCHAR AS email,
    simpleJSONExtractRaw(dados, 'ente_federativo_responsavel')::VARCHAR AS ente_federativo_responsavel,
    simpleJSONExtractRaw(dados, 'identificador_matriz_filial')::VARCHAR AS identificador_matriz_filial,
    simpleJSONExtractRaw(dados, 'logradouro')::VARCHAR AS logradouro,
    simpleJSONExtractRaw(dados, 'motivo_situacao_cadastral')::VARCHAR AS motivo_situacao_cadastral,
    simpleJSONExtractRaw(dados, 'municipio')::VARCHAR AS municipio,
    simpleJSONExtractRaw(dados, 'natureza_juridica')::VARCHAR AS natureza_juridica,
    simpleJSONExtractRaw(dados, 'nome_cidade_no_exterior')::VARCHAR AS nome_cidade_no_exterior,
    simpleJSONExtractRaw(dados, 'nome_fantasia')::VARCHAR AS nome_fantasia,
    simpleJSONExtractRaw(dados, 'numero')::VARCHAR AS numero,
    simpleJSONExtractRaw(dados, 'opcao_pelo_mei')::BOOL AS opcao_pelo_mei,
    simpleJSONExtractRaw(dados, 'opcao_pelo_simples')::BOOL AS opcao_pelo_simples,
    simpleJSONExtractRaw(dados, 'pais')::VARCHAR AS pais,
    simpleJSONExtractRaw(dados, 'porte')::VARCHAR AS porte,
    simpleJSONExtractRaw(dados, 'qualificacao_do_responsavel')::VARCHAR AS qualificacao_do_responsavel,
    simpleJSONExtractRaw(dados, 'razao_social')::VARCHAR AS razao_social,
    simpleJSONExtractRaw(dados, 'situacao_cadastral')::VARCHAR AS situacao_cadastral,
    simpleJSONExtractRaw(dados, 'situacao_especial')::VARCHAR AS situacao_especial,
    simpleJSONExtractRaw(dados, 'uf')::VARCHAR AS uf
FROM
    receita_federal.raw_pessoa_juridica);


CREATE materialized VIEW receita_federal.socio ENGINE = MergeTree() ORDER BY cnpj AS 
(
    WITH socio AS (
        SELECT
            simpleJSONExtractRaw(JSONExtractArrayRaw(dados, 'qsa')[num], 'cnpj_cpf_do_socio') AS cnpj_cpf_do_socio,
            simpleJSONExtractRaw(JSONExtractArrayRaw(dados, 'qsa')[num], 'codigo_faixa_etaria') AS codigo_faixa_etaria,
            simpleJSONExtractRaw(JSONExtractArrayRaw(dados, 'qsa')[num], 'codigo_pais') AS codigo_pais,
            simpleJSONExtractRaw(JSONExtractArrayRaw(dados, 'qsa')[num], 'codigo_qualificacao_representante_legal') AS codigo_qualificacao_representante_legal,
            simpleJSONExtractRaw(JSONExtractArrayRaw(dados, 'qsa')[num], 'codigo_qualificacao_socio') AS codigo_qualificacao_socio,
            simpleJSONExtractRaw(JSONExtractArrayRaw(dados, 'qsa')[num], 'cpf_representante_legal') AS cpf_representante_legal,
            simpleJSONExtractRaw(JSONExtractArrayRaw(dados, 'qsa')[num], 'data_entrada_sociedade') AS data_entrada_sociedade,
            simpleJSONExtractRaw(JSONExtractArrayRaw(dados, 'qsa')[num], 'faixa_etaria') AS faixa_etaria,
            simpleJSONExtractRaw(JSONExtractArrayRaw(dados, 'qsa')[num], 'identificador_socio') AS identificador_socio,
            simpleJSONExtractRaw(JSONExtractArrayRaw(dados, 'qsa')[num], 'nome_representante_legal') AS nome_representante_legal,
            simpleJSONExtractRaw(JSONExtractArrayRaw(dados, 'qsa')[num], 'nome_socio') AS nome_socio,
            simpleJSONExtractRaw(JSONExtractArrayRaw(dados, 'qsa')[num], 'pais') AS pais,
            simpleJSONExtractRaw(JSONExtractArrayRaw(dados, 'qsa')[num], 'qualificacao_representante_legal') AS qualificacao_representante_legal,
            simpleJSONExtractRaw(JSONExtractArrayRaw(dados, 'qsa')[num], 'qualificacao_socio') AS qualificacao_socio,
            cnpj
        FROM receita_federal.raw_pessoa_juridica 
ARRAY JOIN arrayEnumerate(JSONExtractArrayRaw(dados, 'qsa')) AS num
    )
    SELECT
        LPAD(
            cnpj,
            14,
            '0'
        ) AS cnpj,
        CASE
            WHEN pessoa_fisica.cpf IS NOT NULL THEN pessoa_fisica.cpf
            ELSE socio.cnpj_cpf_do_socio::VARCHAR
        END AS documento_socio,
        codigo_faixa_etaria,
        codigo_pais,
        codigo_qualificacao_representante_legal,
        codigo_qualificacao_socio,
        cpf_representante_legal,
        data_entrada_sociedade,
        faixa_etaria,
        identificador_socio,
        nome_representante_legal,
        nome_socio,
        pais,
        qualificacao_representante_legal,
        qualificacao_socio
    FROM
        socio
        LEFT JOIN receita_federal.pessoa_fisica
        ON socio.nome_socio = pessoa_fisica.nome
        AND SUBSTRING(
            socio.cnpj_cpf_do_socio::VARCHAR,
            4,
            9
        ) = SUBSTRING(
            pessoa_fisica.cpf,
            4,
            9
        ));

CREATE TABLE IF NOT EXISTS receita_federal.parentesco
(
	cpf1 VARCHAR(11) 
	,relacao VARCHAR(32) 
	,cpf2 VARCHAR(11) 
	,fonte VARCHAR(256) 
	,tipo VARCHAR(32) 
)
ENGINE = MergeTree()
ORDER BY (cpf1, cpf2);
