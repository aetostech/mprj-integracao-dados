CREATE DATABASE tjrj;

-- tjrj.obito definition

CREATE TABLE tjrj.obito
(

    `nome` String,
    `data_nascimento` Date,
    `data_obito` Date,
    `nome_pai` String,
    `nome_mae` String,
    `servico` String,
    `livro` String,
    `folha` String,
    `termo` String
)
ENGINE = MergeTree
ORDER BY nome
SETTINGS index_granularity = 8192;
