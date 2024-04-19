CREATE DATABASE sistema_financeiro;

-- sistema_financeiro.bcb_inabilitado_proibido definition

CREATE TABLE sistema_financeiro.bcb_inabilitado_proibido
(

    `documento` String,
    `final_penalidade` String,
    `inicio_cumprimento` String,
    `penalidade` String,
    `nome` String,
    `pas` String,
    `prazo_em_anos` Float32
)
ENGINE = MergeTree
ORDER BY documento
SETTINGS index_granularity = 8192;


-- sistema_financeiro.bcb_instituicao_financeira definition

CREATE TABLE sistema_financeiro.bcb_instituicao_financeira
(

    `cnpj_raiz` String,

    `segmento` String,

    `data_observacao` Date
)
ENGINE = MergeTree
ORDER BY cnpj_raiz
SETTINGS index_granularity = 8192;


-- sistema_financeiro.coaf_processo_administrativo_sancionador definition

CREATE TABLE sistema_financeiro.coaf_processo_administrativo_sancionador
(

    `documento_interessado` String,

    `decisao` String,

    `interessados` String,

    `numero_processo` String,

    `link` String,

    `data_julgamento` Date,

    `data_observacao` Date
)
ENGINE = MergeTree
ORDER BY documento_interessado
SETTINGS index_granularity = 8192;


-- sistema_financeiro.cvm_afastamento_temporario definition

CREATE TABLE sistema_financeiro.cvm_afastamento_temporario
(

    `link` String,

    `tempo_vigencia` String,

    `tipo_decisao` String,

    `nome` String,

    `numero_processo` String,

    `data_vigencia` Date,

    `data_julgamento` Date
)
ENGINE = MergeTree
ORDER BY nome
SETTINGS index_granularity = 8192;
