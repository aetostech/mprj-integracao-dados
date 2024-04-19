CREATE DATABASE bnmp;

-- bnmp.mandados definition

CREATE TABLE bnmp.mandados
(

    `nome` String,

    `id` String,

    `metodo_identificacao_cpf` String,

    `pais_nascimento` String,

    `uf_nascimento` String,

    `orgao_judiciario_uf` String,

    `tempo_pena_dia` String,

    `tempo_pena_mes` String,

    `tempo_pena_ano` String,

    `orgao_expedidor_uf` String,

    `regime_prisional` String,

    `tipo_prisao` String,

    `recaptura` String,

    `tipificacao` String,

    `cpf` String,

    `sintese_decisao` String,

    `orgao_judiciario_municipio` String,

    `orgao_judiciario` String,

    `orgao_expedidor_municipio` String,

    `orgao_expedidor` String,

    `magistrado` String,

    `numero_mandado_prisao_anterior` String,

    `registro_judicial_individual` String,

    `sexo` String,

    `municipio_nascimento` String,

    `alcunha` String,

    `nome_pai` String,

    `nome_mae` String,

    `id_pessoa` String,

    `numero_processo` String,

    `status` String,

    `tipo_peca` String,

    `numero_mandado_prisao` String,

    `data_visto_em` Date,

    `data_raspagem` Date,

    `data_validade` Date,

    `data_expedicao` Date,

    `data_nascimento` Date,

    `tipificacoes` String
)
ENGINE = MergeTree
ORDER BY cpf
SETTINGS index_granularity = 8192;
