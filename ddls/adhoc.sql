CREATE DATABASE adhoc;

-- adhoc.bairro definition

CREATE TABLE adhoc.bairro
(

    `sigla_uf` String,

    `nome_bairro` String,

    `nome_municipio` String,

    `coordenada` String,

    `longitude` Float32,

    `latitude` Float32
)
ENGINE = MergeTree
ORDER BY sigla_uf
SETTINGS index_granularity = 8192;


-- adhoc.bairros_baixa_renda definition

CREATE TABLE adhoc.bairros_baixa_renda
(

    `municipio` String,

    `uf` String,

    `bairro` String,

    `porcentagem_ate_um_salario` Float32
)
ENGINE = MergeTree
ORDER BY uf
SETTINGS index_granularity = 8192;


-- adhoc.municipio definition

CREATE TABLE adhoc.municipio
(

    `codigo_municipio` Int32,

    `codigo_uf` Int32,

    `sigla_uf` String,

    `nome_municipio` String,

    `nome_uf` String,

    `coordenada` String,

    `longitude` Float32,

    `latitude` Float32
)
ENGINE = MergeTree
ORDER BY sigla_uf
SETTINGS index_granularity = 8192;
