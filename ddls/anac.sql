CREATE DATABASE anac;

-- anac.rab definition

CREATE TABLE anac.rab
(

    `dt_matricula` String,

    `ds_gravame` String,

    `cd_marca_estrangeira` String,

    `cd_marca_nac3` String,

    `cd_marca_nac2` String,

    `cd_marca_nac1` String,

    `cd_interdicao` String,

    `ds_motivo_canc` String,

    `dt_canc` String,

    `dt_validade_ca` String,

    `dt_validade_cva` String,

    `nr_ano_fabricacao` String,

    `nr_assentos` String,

    `nr_passageiros_max` String,

    `nr_tripulacao_min` String,

    `cd_tipo_icao` String,

    `nr_pmd` String,

    `cd_cls` String,

    `nm_fabricante` String,

    `ds_modelo` String,

    `cd_tipo` String,

    `cd_categoria` String,

    `nr_serie` String,

    `nr_cert_matricula` String,

    `cpf_cgc` String,

    `uf_operador` String,

    `outros_operadores` String,

    `nm_operador` String,

    `cpf_cnpj` String,

    `sg_uf` String,

    `outros_proprietarios` String,

    `proprietario` String,

    `marca` String
)
ENGINE = MergeTree
ORDER BY marca
SETTINGS index_granularity = 8192;
