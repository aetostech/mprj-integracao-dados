CREATE DATABASE tse;

-- tse.candidatura definition

CREATE TABLE tse.candidatura
(

    `eleito` Bool,

    `ano` Int32,

    `numero` Int32,

    `cpf` String,

    `cargo` String,

    `partido` String,

    `municipio` String,

    `uf` String,

    `nome` String,

    `titulo_eleitor` String
)
ENGINE = MergeTree
ORDER BY cpf
SETTINGS index_granularity = 8192;


-- tse.despesa_campanha definition

CREATE TABLE tse.despesa_campanha
(

    `eleito` Bool,

    `numero_candidato` Int32,

    `ano` Int32,

    `documento_fornecedor` String,

    `nome_candidato` String,

    `cargo` String,

    `partido` String,

    `cpf_candidato` String,

    `municipio` String,

    `uf` String,

    `valor_despesa` Float32
)
ENGINE = MergeTree
ORDER BY documento_fornecedor
SETTINGS index_granularity = 8192;


-- tse.doacao_campanha definition

CREATE TABLE tse.doacao_campanha
(
    `eleito` Bool,
    `cargo` String,
    `uf` String,
    `partido` String,
    `nome_candidato` String,
    `cpf_candidato` String,
    `municipio` String,
    `nome_doador` String,
    `documento_doador` String,
    `valor_doacao` Float32,
    `ano` Float32
)
ENGINE = MergeTree
ORDER BY documento_doador
SETTINGS index_granularity = 8192;
