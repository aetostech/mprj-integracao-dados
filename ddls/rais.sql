CREATE DATABASE rais;

-- rais.cbo definition

CREATE TABLE rais.cbo
(

    `profissao_baixa_qualificacao` Bool,

    `cbo` String,

    `descricao` String
)
ENGINE = MergeTree
ORDER BY cbo
SETTINGS index_granularity = 8192;


-- rais.funcionario definition

CREATE TABLE rais.funcionario
(

    `funcionario_publico` Bool,

    `ano` Int32,

    `qtde_horas_contratadas` Int32,

    `n_meses_rem` Int32,

    `codigo_municipio` Int32,

    `cpf` String,

    `cbo` String,

    `cnpj` String,

    `uf` String,

    `sexo` String,

    `nome_fantasia` String,

    `razao_social` String,

    `nome_trabalhador` String,

    `municipio` String,

    `pis` String,

    `data_admissao` Date,

    `data_nascimento` Date,

    `vl_rem_contratual` Float32,

    `vl_rem_novembro` Float32,

    `vl_rem_janeiro` Float32,

    `vl_rem_fevereiro` Float32,

    `vl_rem_dezembro` Float32,

    `vl_rem_marco` Float32,

    `vl_rem_abril` Float32,

    `vl_rem_maio` Float32,

    `vl_rem_julho` Float32,

    `vl_rem_setembro` Float32,

    `vl_rem_outubro` Float32,

    `vl_rem_junho` Float32,

    `vl_rem_agosto` Float32
)
ENGINE = MergeTree
ORDER BY cpf
SETTINGS index_granularity = 8192;


-- rais.n_funcionarios_lai definition

CREATE TABLE rais.n_funcionarios_lai
(

    `n_vinculos` Int32,

    `ano` Int32,

    `cnpj` String
)
ENGINE = MergeTree
ORDER BY cnpj
SETTINGS index_granularity = 8192;
