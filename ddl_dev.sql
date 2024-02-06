CREATE SCHEMA "anac";

CREATE SCHEMA "adhoc";

CREATE SCHEMA "sistema_financeiro";

CREATE SCHEMA "transparencia";

CREATE SCHEMA "tse";

CREATE SCHEMA "rais";

CREATE SCHEMA "receita";

CREATE SCHEMA "receita_federal";

CREATE SCHEMA "bnmp";

CREATE TABLE "anac"."rab" (
  "dt_matricula" "character varying",
  "ds_gravame" "character varying",
  "cd_marca_estrangeira" "character varying",
  "cd_marca_nac3" "character varying",
  "cd_marca_nac2" "character varying",
  "cd_marca_nac1" "character varying",
  "cd_interdicao" "character varying",
  "ds_motivo_canc" "character varying",
  "dt_canc" "character varying",
  "dt_validade_ca" "character varying",
  "dt_validade_cva" "character varying",
  "nr_ano_fabricacao" "character varying",
  "nr_assentos" "character varying",
  "nr_passageiros_max" "character varying",
  "nr_tripulacao_min" "character varying",
  "cd_tipo_icao" "character varying",
  "nr_pmd" "character varying",
  "cd_cls" "character varying",
  "nm_fabricante" "character varying",
  "ds_modelo" "character varying",
  "cd_tipo" "character varying",
  "cd_categoria" "character varying",
  "nr_serie" "character varying",
  "nr_cert_matricula" "character varying",
  "cpf_cgc" "character varying",
  "uf_operador" "character varying",
  "outros_operadores" "character varying",
  "nm_operador" "character varying",
  "cpf_cnpj" "character varying",
  "sg_uf" "character varying",
  "outros_proprietarios" "character varying",
  "proprietario" "character varying",
  "marca" "character varying"
);

CREATE TABLE "adhoc"."bairro" (
  "sigla_uf" "character ",
  "nome_bairro" "character varying",
  "nome_municipio" "character varying",
  "coordenada" geography,
  "longitude" doubleprecision,
  "latitude" doubleprecision
);

CREATE TABLE "adhoc"."bairros_baixa_renda" (
  "municipio" "character varying",
  "uf" "character varying",
  "bairro" "character varying",
  "porcentagem_ate_um_salario" doubleprecision
);

CREATE TABLE "adhoc"."municipio" (
  "codigo_municipio" integer,
  "codigo_uf" integer,
  "sigla_uf" "character ",
  "nome_municipio" "character varying",
  "nome_uf" "character varying",
  "coordenada" geography,
  "longitude" doubleprecision,
  "latitude" doubleprecision
);

CREATE TABLE "sistema_financeiro"."bcb_inabilitado_proibido" (
  "documento" "character varying",
  "final_penalidade" "character varying",
  "inicio_cumprimento" "character varying",
  "penalidade" "character varying",
  "nome" "character varying",
  "pas" "character varying",
  "prazo_em_anos" doubleprecision
);

CREATE TABLE "sistema_financeiro"."bcb_instituicao_financeira" (
  "cnpj_raiz" "character ",
  "segmento" "character varying",
  "data_observacao" date
);

CREATE TABLE "sistema_financeiro"."coaf_processo_administrativo_sancionador" (
  "documento_interessado" "character varying",
  "decisao" "character varying",
  "interessados" "character varying",
  "numero_processo" "character varying",
  "link" "character varying",
  "data_julgamento" date,
  "data_observacao" date
);

CREATE TABLE "sistema_financeiro"."cvm_afastamento_temporario" (
  "link" "character varying",
  "tempo_vigencia" "character varying",
  "tipo_decisao" "character varying",
  "nome" "character varying",
  "numero_processo" "character varying",
  "data_vigencia" date,
  "data_julgamento" date
);

CREATE TABLE "transparencia"."beneficios_sociais" (
  "ano_referencia" integer,
  "mes_referencia" integer,
  "nis_beneficiario" "character ",
  "cpf_beneficiario_anonimizado" "character ",
  "cpf_representante_legal_anonimizado" "character ",
  "estado" "character ",
  "nome_beneficiario" "character varying",
  "nome_representante_legal" "character varying",
  "municipio" "character varying",
  "id_tipo_beneficio" "character varying",
  "valor_parcela" doubleprecision
);

CREATE TABLE "transparencia"."pessoa_exposta_politicamente" (
  "cpf_mascarado" "character ",
  "sigla_funcao" "character varying",
  "nome_orgao" "character varying",
  "nivel_funcao" "character varying",
  "descricao_funcao" "character varying",
  "nome" "character varying",
  "data_coleta" date,
  "data_fim_carencia" date,
  "data_fim_exercicio" date,
  "data_inicio_exercicio" date
);

CREATE TABLE "transparencia"."pgfn_devedor" (
  "estado" "character varying",
  "tipo_divida" "character varying",
  "nome_devedor" "character varying",
  "documento_devedor" "character varying",
  "valor_divida" real,
  "data_pesquisa" timestamp
);

CREATE TABLE "transparencia"."punicao" (
  "documento" "character varying",
  "descricao_fundamentacao" "character varying",
  "base_origem" "character varying",
  "tipo_punicao" "character varying",
  "tipo_documento" "character varying",
  "observacoes" "character varying",
  "abrangencia" "character varying",
  "uf_punicao" "character varying",
  "orgao_punicao" "character varying",
  "data_coleta" date,
  "fim_punicao" date,
  "inicio_punicao" date
);

CREATE TABLE "transparencia"."tipo_beneficio" (
  "nome" "character varying",
  "id" "character varying"
);

CREATE TABLE "tse"."candidatura" (
  "eleito" boolean,
  "ano" integer,
  "numero" integer,
  "cpf" "character ",
  "cargo" "character varying",
  "partido" "character varying",
  "municipio" "character varying",
  "uf" "character varying",
  "nome" "character varying",
  "titulo_eleitor" "character varying"
);

CREATE TABLE "tse"."despesa_campanha" (
  "eleito" boolean,
  "numero_candidato" integer,
  "ano" integer,
  "documento_fornecedor" "character varying",
  "nome_candidato" "character varying",
  "cargo" "character varying",
  "partido" "character varying",
  "cpf_candidato" "character varying",
  "municipio" "character varying",
  "uf" "character varying",
  "valor_despesa" doubleprecision
);

CREATE TABLE "tse"."doacao_campanha" (
  "eleito" boolean,
  "cargo" "character varying",
  "uf" "character varying",
  "partido" "character varying",
  "nome_candidato" "character varying",
  "cpf_candidato" "character varying",
  "municipio" "character varying",
  "nome_doador" "character varying",
  "documento_doador" "character varying",
  "valor_doacao" doubleprecision,
  "ano" doubleprecision
);

CREATE TABLE "rais"."cbo" (
  "profissao_baixa_qualificacao" boolean,
  "cbo" "character ",
  "descricao" "character varying"
);

CREATE TABLE "rais"."funcionario" (
  "funcionario_publico" boolean,
  "ano" integer,
  "qtde_horas_contratadas" integer,
  "n_meses_rem" integer,
  "codigo_municipio" integer,
  "cpf" "character ",
  "cbo" "character ",
  "cnpj" "character ",
  "uf" "character ",
  "sexo" "character ",
  "nome_fantasia" "character varying",
  "razao_social" "character varying",
  "nome_trabalhador" "character varying",
  "municipio" "character varying",
  "pis" "character varying",
  "data_admissao" date,
  "data_nascimento" date,
  "vl_rem_contratual" doubleprecision,
  "vl_rem_novembro" doubleprecision,
  "vl_rem_janeiro" doubleprecision,
  "vl_rem_fevereiro" doubleprecision,
  "vl_rem_dezembro" doubleprecision,
  "vl_rem_marco" doubleprecision,
  "vl_rem_abril" doubleprecision,
  "vl_rem_maio" doubleprecision,
  "vl_rem_julho" doubleprecision,
  "vl_rem_setembro" doubleprecision,
  "vl_rem_outubro" doubleprecision,
  "vl_rem_junho" doubleprecision,
  "vl_rem_agosto" doubleprecision
);

CREATE TABLE "rais"."n_funcionarios_lai" (
  "n_vinculos" integer,
  "ano" integer,
  "cnpj" "character "
);

CREATE TABLE "receita"."contador" (
  "num_registro_crc" integer,
  "num_telefone" integer,
  "num_ddd" integer,
  "num_cep" integer,
  "ind_matriz_filial" integer,
  "ind_tipo" "character varying",
  "tipo_logradouro" "character varying",
  "cod_tipo_logradouro" "character varying",
  "sigla_uf_crc" "character varying",
  "sigla_uf" "character varying",
  "num_cnpj_empresa" "character varying",
  "descr_email" "character varying",
  "nome_municipio" "character varying",
  "nome_bairro" "character varying",
  "descr_complemento_logradouro" "character varying",
  "num_logradouro" "character varying",
  "descr_logradouro" "character varying",
  "nome" "character varying",
  "num_cpf" "character varying",
  "num_cnpj" "character varying"
);

CREATE TABLE "receita_federal"."pessoa_fisica" (
  "datanascimento" integer,
  "exercicioocupacao" integer,
  "sexo" integer,
  "ocupacaoprincipal" integer,
  "naturezaocupacao" integer,
  "nome" "character varying",
  "cpf" "character varying",
  "tipologradouro" "character varying",
  "tituloeleitor" "character varying",
  "dataatualizacao" "character varying",
  "estrangeiro" "character varying",
  "telefone" "character varying",
  "ddd" "character varying",
  "situacaocadastral" "character varying",
  "residenteexterior" "character varying",
  "codigopaisexterior" "character varying",
  "nomepaisexterior" "character varying",
  "cpfmascarado" "character varying",
  "unidadeadministrativa" "character varying",
  "municipio" "character varying",
  "codigomunicipio" "character varying",
  "uf" "character varying",
  "cep" "character varying",
  "bairro" "character varying",
  "complemento" "character varying",
  "numerologradouro" "character varying",
  "logradouro" "character varying",
  "nomemae" "character varying",
  "pessoafisicaid" bigint,
  "anoobito" doubleprecision
);

CREATE TABLE "receita_federal"."socio" (
  "cnpj" STRING,
  "documento_socio_anonimizado" STRING,
  "codigo_faixa_etaria" STRING,
  "codigo_pais" STRING,
  "codigo_qualificacao_representante_legal" STRING,
  "codigo_qualificacao_socio" STRING,
  "cpf_representante_legal" STRING,
  "data_entrada_sociedade" STRING,
  "faixa_etaria" STRING,
  "identificador_socio" STRING,
  "nome_representante_legal" STRING,
  "nome_socio" STRING,
  "pais" STRING,
  "qualificacao_representante_legal" STRING,
  "qualificacao_socio" STRING
);

CREATE TABLE "receita_federal"."pessoa_juridica" (
  "cnpj" STRING,
  "bairro" STRING,
  "capital_social" STRING,
  "cep" STRING,
  "cnae_fiscal" STRING,
  "cnae_fiscal_descricao" STRING,
  "cnaes_secundarios" JSON,
  "codigo_municipio" STRING,
  "codigo_municipio_ibge" STRING,
  "codigo_natureza_juridica" STRING,
  "codigo_pais" STRING,
  "codigo_porte" STRING,
  "complemento" STRING,
  "data_exclusao_do_mei" STRING,
  "data_exclusao_do_simples" STRING,
  "data_inicio_atividade" STRING,
  "data_opcao_pelo_mei" STRING,
  "data_opcao_pelo_simples" STRING,
  "data_situacao_cadastral" STRING,
  "data_situacao_especial" STRING,
  "ddd_fax" STRING,
  "ddd_telefone_1" STRING,
  "ddd_telefone_2" STRING,
  "descricao_identificador_matriz_filial" STRING,
  "descricao_motivo_situacao_cadastral" STRING,
  "descricao_porte" STRING,
  "descricao_situacao_cadastral" STRING,
  "descricao_tipo_de_logradouro" STRING,
  "email" STRING,
  "ente_federativo_responsavel" STRING,
  "identificador_matriz_filial" STRING,
  "logradouro" STRING,
  "motivo_situacao_cadastral" STRING,
  "municipio" STRING,
  "natureza_juridica" STRING,
  "nome_cidade_no_exterior" STRING,
  "nome_fantasia" STRING,
  "numero" STRING,
  "opcao_pelo_mei" STRING,
  "opcao_pelo_simples" STRING,
  "pais" STRING,
  "porte" STRING,
  "qualificacao_do_responsavel" STRING,
  "razao_social" STRING,
  "situacao_cadastral" STRING,
  "situacao_especial" STRING,
  "uf" STRING
);

CREATE TABLE "bnmp"."mandados" (
  "nome" "character varying",
  "id" "character varying",
  "metodo_identificacao_cpf" "character varying",
  "pais_nascimento" "character varying",
  "uf_nascimento" "character varying",
  "orgao_judiciario_uf" "character varying",
  "tempo_pena_dia" "character varying",
  "tempo_pena_mes" "character varying",
  "tempo_pena_ano" "character varying",
  "orgao_expedidor_uf" "character varying",
  "regime_prisional" "character varying",
  "tipo_prisao" "character varying",
  "recaptura" "character varying",
  "tipificacao" "character varying",
  "cpf" "character varying",
  "sintese_decisao" "character varying",
  "orgao_judiciario_municipio" "character varying",
  "orgao_judiciario" "character varying",
  "orgao_expedidor_municipio" "character varying",
  "orgao_expedidor" "character varying",
  "magistrado" "character varying",
  "numero_mandado_prisao_anterior" "character varying",
  "registro_judicial_individual" "character varying",
  "sexo" "character varying",
  "municipio_nascimento" "character varying",
  "alcunha" "character varying",
  "nome_pai" "character varying",
  "nome_mae" "character varying",
  "id_pessoa" "character varying",
  "numero_processo" "character varying",
  "status" "character varying",
  "tipo_peca" "character varying",
  "numero_mandado_prisao" "character varying",
  "data_visto_em" date,
  "data_raspagem" date,
  "data_validade" date,
  "data_expedicao" date,
  "data_nascimento" date,
  "tipificacoes" super
);
