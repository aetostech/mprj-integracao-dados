CREATE DATABASE transparencia;

-- transparencia.beneficios_sociais definition

CREATE TABLE transparencia.beneficios_sociais
(
    `ano_referencia` Int32,
    `mes_referencia` Int32,
    `nis_beneficiario` String,
    `cpf_beneficiario_anonimizado` String,
    `cpf_representante_legal_anonimizado` String,
    `estado` String,
    `nome_beneficiario` String,
    `nome_representante_legal` String,
    `municipio` String,
    `id_tipo_beneficio` String,
    `valor_parcela` Float32
)
ENGINE = MergeTree
ORDER BY nis_beneficiario
SETTINGS index_granularity = 8192;


-- transparencia.pessoa_exposta_politicamente definition

CREATE TABLE transparencia.pessoa_exposta_politicamente
(

    `cpf_mascarado` String,

    `sigla_funcao` String,

    `nome_orgao` String,

    `nivel_funcao` String,

    `descricao_funcao` String,

    `nome` String,

    `data_coleta` Date,

    `data_fim_carencia` Date,

    `data_fim_exercicio` Date,

    `data_inicio_exercicio` Date
)
ENGINE = MergeTree
ORDER BY cpf_mascarado
SETTINGS index_granularity = 8192;


-- transparencia.pgfn_devedor definition

CREATE TABLE transparencia.pgfn_devedor
(

    `estado` String,

    `tipo_divida` String,

    `nome_devedor` String,

    `documento_devedor` String,

    `valor_divida` Float32,

    `data_pesquisa` Date
)
ENGINE = MergeTree
ORDER BY nome_devedor
SETTINGS index_granularity = 8192;


-- transparencia.punicao definition

CREATE TABLE transparencia.punicao
(

    `documento` String,

    `descricao_fundamentacao` String,

    `base_origem` String,

    `tipo_punicao` String,

    `tipo_documento` String,

    `observacoes` String,

    `abrangencia` String,

    `uf_punicao` String,

    `orgao_punicao` String,

    `data_coleta` Date,

    `fim_punicao` Date,

    `inicio_punicao` Date
)
ENGINE = MergeTree
ORDER BY documento
SETTINGS index_granularity = 8192;


-- transparencia.tipo_beneficio definition

CREATE TABLE transparencia.tipo_beneficio
(

    `nome` String,

    `id` String
)
ENGINE = MergeTree
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192;
