CREATE DATABASE etl_manual;

CREATE TABLE IF NOT EXISTS etl_manual.mte_trabalho_escravo
(
	ano VARCHAR(256)
	,uf VARCHAR(256)
	,municipio VARCHAR(256)
	,cnae VARCHAR(256)
	,descricao_cnae VARCHAR(256)
	,cnpj_cei_cpf VARCHAR(256)
	,estabelecimento_inspecionado VARCHAR(256)
	,n_trab_menor_16 VARCHAR(256)
	,n_trab_16_a_18 VARCHAR(256)
	,n_trab_cond_analog_escravo VARCHAR(256)
)
ENGINE = MergeTree()
ORDER BY ano;

CREATE TABLE IF NOT EXISTS etl_manual.selecionados
(
	documento VARCHAR(256) NOT NULL
	,id_no VARCHAR(256) NOT NULL
	,fonte String NOT NULL
	,"data" DATE NOT NULL
)
ENGINE = MergeTree()
PRIMARY KEY (documento, id_no);

CREATE TABLE IF NOT EXISTS etl_manual.hiri
(
	cpf VARCHAR(256)
	,id_no VARCHAR(256)
	,fonte VARCHAR(256)
)
ENGINE = MergeTree()
PRIMARY KEY (id_no);

CREATE TABLE IF NOT EXISTS etl_manual.ecnm
(
	documento VARCHAR(256)
	,id_no VARCHAR(256)
	,fonte VARCHAR(256)
)
ENGINE = MergeTree()
PRIMARY KEY (id_no);

CREATE TABLE IF NOT EXISTS etl_manual.pandora
(
	cpf VARCHAR(256)
	,id_no VARCHAR(256)
	,fonte VARCHAR(256)
)
ENGINE = MergeTree()
PRIMARY KEY (id_no);

CREATE TABLE IF NOT EXISTS etl_manual.empresas_fantasma
(
	cnpj VARCHAR(256)
)
ENGINE = MergeTree()
PRIMARY KEY (cnpj);

CREATE TABLE IF NOT EXISTS etl_manual.pep
(
	documento VARCHAR(256)
	,tipo VARCHAR(256)
)
ENGINE = MergeTree()
PRIMARY KEY (documento);