WITH fornecedor_base AS (
    SELECT DISTINCT contrato.id_fornecedor,
        contrato.id_base
    FROM {{ source('unibase', 'contrato') }}
    UNION
    SELECT DISTINCT proposta.id_fornecedor,
        proposta.id_base
    FROM {{ source('unibase', 'proposta') }}
),
base_supplier AS (
    SELECT fb.id_base,
        fb.id_fornecedor,
        f.documento AS cnpj
    FROM {{ source('unibase', 'fornecedor') }} f
        JOIN fornecedor_base fb ON f.id_fornecedor = fb.id_fornecedor
),
gen_info AS (
    SELECT f.id_fornecedor,
        pj.data_inicio_atividade AS data_constituicao,
        pj.capital_social,
        pj.uf
    FROM {{ source('dados_cadastrais', 'pessoa_juridica') }} pj
        JOIN {{ source('unibase', 'fornecedor') }} f ON pj.cnpj = f.documento::bpchar
),
company_name AS (
    SELECT pessoa_juridica.cnpj,
        pessoa_juridica.razao_social AS nome
    FROM {{ source('dados_cadastrais', 'pessoa_juridica') }}
),
prop_info AS (
    SELECT l.id_base,
        p.id_fornecedor,
        COUNT(DISTINCT p.id_proposta) AS n_propostas,
        COUNT(DISTINCT l.id_licitacao) AS n_licitacoes,
        SUM(p.valor) AS valor_total_propostas,
        SUM(p.proposta_ganhadora::INTEGER) AS n_propostas_ganhadoras,
        SUM((NOT p.proposta_ganhadora)::INTEGER) AS n_propostas_perdedoras,
        SUM(
            CASE
                WHEN p.proposta_ganhadora THEN p.valor
                ELSE 0::DOUBLE PRECISION
            END
        ) AS valor_total_propostas_ganhadoras,
        AVG(l.num_propostas) AS n_medio_licitantes,
        CASE
            WHEN (
                SUM(p.proposta_ganhadora::INTEGER) + SUM((NOT p.proposta_ganhadora)::INTEGER)
            ) = 0 THEN 0::NUMERIC
            ELSE ROUND(
                SUM(p.proposta_ganhadora::INTEGER)::NUMERIC / (
                    1.0 * SUM(p.proposta_ganhadora::INTEGER)::NUMERIC + SUM((NOT p.proposta_ganhadora)::INTEGER)::NUMERIC
                ),
                3
            )
        END AS taxa_vitorias,
        MIN(p.data) AS data_primeira_proposta,
        MAX(p.data) AS data_ultima_proposta,
        SUM(
            CASE
                WHEN l.num_propostas = 1 THEN 1
                ELSE 0
            END
        ) AS n_propostas_um_licitante,
        SUM(
            CASE
                WHEN l.num_propostas = 1 THEN p.valor
                ELSE 0::DOUBLE PRECISION
            END
        ) AS valor_total_propostas_um_licitante,
        SUM(
            CASE
                WHEN l.num_propostas = 1
                AND p.proposta_ganhadora THEN 1
                ELSE 0
            END
        ) AS n_propostas_ganhadoras_um_licitante,
        SUM(
            CASE
                WHEN l.num_propostas = 1
                AND p.proposta_ganhadora THEN p.valor
                ELSE 0::DOUBLE PRECISION
            END
        ) AS valor_total_propostas_ganhadoras_um_licitante
    FROM {{ source('unibase', 'proposta') }} p
        JOIN {{ source('unibase', 'lote') }} l ON l.id_lote = p.id_lote
        AND l.id_base = p.id_base
    WHERE p.id_avoid = 0
    GROUP BY l.id_base,
        p.id_fornecedor
),
cont_info AS (
    SELECT c.id_base,
        c.id_fornecedor,
        COUNT(DISTINCT c.id_contrato) AS n_contratos,
        SUM(c.valor) AS valor_total_contratos,
        MIN(c.data) AS data_primeiro_contrato,
        MAX(c.data) AS data_ultimo_contrato
    FROM {{ source('unibase', 'contrato') }} c
    WHERE c.id_avoid = 0
    GROUP BY c.id_base,
        c.id_fornecedor
),
add_info AS (
    SELECT a.id_base,
        c.id_fornecedor,
        COUNT(DISTINCT a.id_aditivo) AS n_aditivos,
        SUM(a.valor) AS valor_total_aditivos
    FROM {{ source('unibase', 'contrato') }} c
        JOIN {{ source('unibase', 'aditivo') }} a ON a.id_contrato = c.id_contrato
        AND a.id_base = c.id_base
    WHERE a.valor IS NOT NULL
        AND a.id_avoid = 0
    GROUP BY a.id_base,
        c.id_fornecedor
),
partner_info AS (
    SELECT f.id_fornecedor,
        COUNT(*) AS n_socios
    FROM (
            SELECT socio.cnpj,
                socio.documento_socio
            FROM {{ source('dados_cadastrais', 'socio') }}
        ) s
        JOIN {{ source('unibase', 'fornecedor') }} f ON s.cnpj::TEXT = f.documento::TEXT
    GROUP BY f.id_fornecedor
),
general_info AS (
    SELECT fb.id_base,
        fb.id_fornecedor,
        gi.data_constituicao,
        gi.capital_social,
        gi.uf,
        pai.n_socios,
        pri.n_propostas,
        pri.n_licitacoes,
        pri.valor_total_propostas,
        pri.n_propostas_ganhadoras,
        pri.n_propostas_perdedoras,
        pri.n_propostas_um_licitante,
        pri.valor_total_propostas_um_licitante,
        pri.n_propostas_ganhadoras_um_licitante,
        pri.valor_total_propostas_ganhadoras_um_licitante,
        pri.valor_total_propostas_ganhadoras,
        pri.n_medio_licitantes,
        pri.taxa_vitorias,
        pri.data_primeira_proposta,
        pri.data_ultima_proposta,
        ci.n_contratos,
        ci.valor_total_contratos,
        ci.data_primeiro_contrato,
        ci.data_ultimo_contrato,
        ai.n_aditivos,
        ai.valor_total_aditivos,
        CASE
            WHEN ci.valor_total_contratos = 0::DOUBLE PRECISION THEN 0::DOUBLE PRECISION
            ELSE ai.valor_total_aditivos / ci.valor_total_contratos
        END AS majoracao_contratual_via_aditivos
    FROM fornecedor_base fb
        LEFT JOIN gen_info gi ON fb.id_fornecedor = gi.id_fornecedor
        LEFT JOIN prop_info pri ON fb.id_base = pri.id_base
        AND fb.id_fornecedor = pri.id_fornecedor
        LEFT JOIN cont_info ci ON fb.id_base = ci.id_base
        AND fb.id_fornecedor = ci.id_fornecedor
        LEFT JOIN add_info ai ON fb.id_base = ai.id_base
        AND fb.id_fornecedor = ai.id_fornecedor
        LEFT JOIN partner_info pai ON fb.id_fornecedor = pai.id_fornecedor
    WHERE fb.id_fornecedor IS NOT NULL
),
cct_partner_pattern AS (
    SELECT beneficiario.cnpj,
        BOOL_OR(
            beneficiario.socio_beneficiario_bolsa_familia::INTEGER::BOOLEAN
        ) AS socio_beneficiario_bf,
        BOOL_OR(
            beneficiario.socio_beneficiario_seguro_defeso::INTEGER::BOOLEAN
        ) AS socio_beneficiario_seguro_defeso,
        BOOL_OR(
            beneficiario.socio_beneficiario_bpc::INTEGER::BOOLEAN
        ) AS socio_beneficiario_bpc,
        BOOL_OR(
            beneficiario.socio_beneficiario_garantia_safra::INTEGER::BOOLEAN
        ) AS socio_beneficiario_garantia_safra,
        BOOL_OR(
            beneficiario.socio_beneficiario_peti::INTEGER::BOOLEAN
        ) AS socio_beneficiario_peti
    FROM {{ ref('beneficiario') }}
    GROUP BY beneficiario.cnpj
),
punished_company AS (
    SELECT punida.cnpj,
        SUM(
            CASE
                WHEN punida.base_origem::TEXT = 'LTE'::TEXT THEN 1
                ELSE 0
            END
        )::INTEGER::BOOLEAN AS punida_lte,
        SUM(
            CASE
                WHEN punida.base_origem::TEXT = 'CEIS'::TEXT THEN 1
                ELSE 0
            END
        )::INTEGER::BOOLEAN AS punida_ceis,
        SUM(
            CASE
                WHEN punida.base_origem::TEXT = 'CEPIM'::TEXT THEN 1
                ELSE 0
            END
        )::INTEGER::BOOLEAN AS punida_cepim,
        SUM(
            CASE
                WHEN punida.base_origem::TEXT = 'CNEP'::TEXT THEN 1
                ELSE 0
            END
        )::INTEGER::BOOLEAN AS punida_cnep,
        SUM(
            CASE
                WHEN punida.base_origem::TEXT = 'TCE-SP'::TEXT THEN 1
                ELSE 0
            END
        )::INTEGER::BOOLEAN AS punida_tce_sp,
        SUM(
            CASE
                WHEN punida.base_origem::TEXT = 'AL'::TEXT THEN 1
                ELSE 0
            END
        )::INTEGER::BOOLEAN AS punida_al
    FROM {{ ref('punida') }}
    GROUP BY punida.cnpj
),
punished_partner AS (
    SELECT punido.cnpj,
        SUM(
            CASE
                WHEN punido.base_origem::TEXT = 'CEIS'::TEXT THEN 1
                ELSE 0
            END
        )::INTEGER::BOOLEAN AS socio_punido_ceis,
        SUM(
            CASE
                WHEN punido.base_origem::TEXT = 'AL'::TEXT THEN 1
                ELSE 0
            END
        )::INTEGER::BOOLEAN AS socio_punido_al,
        SUM(
            CASE
                WHEN punido.base_origem::TEXT = 'CNEP'::TEXT THEN 1
                ELSE 0
            END
        )::INTEGER::BOOLEAN AS socio_punido_cnep,
        SUM(
            CASE
                WHEN punido.base_origem::TEXT = 'CEPIM'::TEXT THEN 1
                ELSE 0
            END
        )::INTEGER::BOOLEAN AS socio_punido_cepim
    FROM {{ ref('punido') }}
    GROUP BY punido.cnpj
),
n_cnaes AS (
    SELECT pessoa_juridica_cnaes.cnpj,
        COUNT(
            DISTINCT "substring"(pessoa_juridica_cnaes.cnae::TEXT, 1, 2)
        ) AS n_cnaes_divisoes,
        COUNT(
            DISTINCT "substring"(pessoa_juridica_cnaes.cnae::TEXT, 1, 3)
        ) AS n_cnaes_grupos,
        COUNT(
            DISTINCT "substring"(pessoa_juridica_cnaes.cnae::TEXT, 1, 5)
        ) AS n_cnaes_classes,
        COUNT(DISTINCT pessoa_juridica_cnaes.cnae) AS n_cnaes
    FROM {{ source('dados_cadastrais', 'pessoa_juridica_cnaes') }}
        JOIN {{ source('unibase', 'fornecedor') }} ON pessoa_juridica_cnaes.cnpj::TEXT = fornecedor.documento::TEXT
    GROUP BY pessoa_juridica_cnaes.cnpj
),
precocity AS (
    SELECT precocidade.cnpj,
        precocidade.id_base,
        MIN(precocidade.dias_ate_proposta) AS precocidade_proposta_em_dias,
        MIN(precocidade.dias_ate_contrato) AS precocidade_contrato_em_dias
    FROM {{ ref('precocidade') }}
    GROUP BY precocidade.cnpj,
        precocidade.id_base
),
n_employees AS (
    SELECT n_funcionarios_lai.cnpj,
        n_funcionarios_lai.n_funcionarios_2010,
        n_funcionarios_lai.n_funcionarios_2011,
        n_funcionarios_lai.n_funcionarios_2012,
        n_funcionarios_lai.n_funcionarios_2013,
        n_funcionarios_lai.n_funcionarios_2014,
        n_funcionarios_lai.n_funcionarios_2015,
        n_funcionarios_lai.n_funcionarios_2016,
        n_funcionarios_lai.n_funcionarios_2017,
        n_funcionarios_lai.n_funcionarios_2018
    FROM {{ source('dados_cadastrais', 'n_funcionarios_lai') }}
),
n_political_bonds AS (
    SELECT t.cnpj,
        COUNT(DISTINCT t.partido) AS n_vinculos_partidarios
    FROM (
            SELECT politico.cnpj,
                politico.partido
            FROM {{ ref('politico') }}
            WHERE politico.cargo::TEXT <> 'VEREADOR'::TEXT
            UNION
            SELECT doadora_campanha.cnpj,
                doadora_campanha.partido
            FROM {{ ref('doadora_campanha') }}
            WHERE doadora_campanha.cargo::TEXT <> 'VEREADOR'::TEXT
            UNION
            SELECT doador_campanha.cnpj,
                doador_campanha.partido
            FROM {{ ref('doador_campanha') }}
            WHERE doador_campanha.cargo::TEXT <> 'VEREADOR'::TEXT
            UNION
            SELECT fornecedora_campanha.cnpj,
                fornecedora_campanha.partido
            FROM {{ ref('fornecedora_campanha') }}
            WHERE fornecedora_campanha.cargo::TEXT <> 'VEREADOR'::TEXT
            UNION
            SELECT fornecedor_campanha.cnpj,
                fornecedor_campanha.partido
            FROM {{ ref('fornecedor_campanha') }}
            WHERE fornecedor_campanha.cargo::TEXT <> 'VEREADOR'::TEXT
        ) t
    GROUP BY t.cnpj
),
political_parties AS (
    SELECT t.cnpj,
        STRING_AGG(
            t.partido::TEXT,
            '; '::TEXT
            ORDER BY (t.partido::TEXT)
        ) AS vinculo_partidario
    FROM (
            SELECT politico.cnpj,
                politico.partido
            FROM {{ ref('politico') }}
            WHERE politico.cargo::TEXT <> 'VEREADOR'::TEXT
            UNION
            SELECT doadora_campanha.cnpj,
                doadora_campanha.partido
            FROM {{ ref('doadora_campanha') }}
            WHERE doadora_campanha.cargo::TEXT <> 'VEREADOR'::TEXT
            UNION
            SELECT doador_campanha.cnpj,
                doador_campanha.partido
            FROM {{ ref('doador_campanha') }}
            WHERE doador_campanha.cargo::TEXT <> 'VEREADOR'::TEXT
            UNION
            SELECT fornecedora_campanha.cnpj,
                fornecedora_campanha.partido
            FROM {{ ref('fornecedora_campanha') }}
            WHERE fornecedora_campanha.cargo::TEXT <> 'VEREADOR'::TEXT
            UNION
            SELECT fornecedor_campanha.cnpj,
                fornecedor_campanha.partido
            FROM {{ ref('fornecedor_campanha') }}
            WHERE fornecedor_campanha.cargo::TEXT <> 'VEREADOR'::TEXT
        ) t
    GROUP BY t.cnpj
),
n_competitors AS (
    SELECT p1.id_base,
        p1.id_fornecedor,
        COUNT(DISTINCT p2.id_fornecedor) AS n_licitantes_contra
    FROM {{ source('unibase', 'proposta') }} p1
        JOIN {{ source('unibase', 'proposta') }} p2 ON p1.id_lote = p2.id_lote
    WHERE p1.id_fornecedor <> p2.id_fornecedor
    GROUP BY p1.id_base,
        p1.id_fornecedor
),
bool_patterns AS (
    SELECT DISTINCT base_supplier_1.cnpj,
        base_supplier_1.id_base,
        (
            base_supplier_1.cnpj::TEXT IN (
                SELECT politico.cnpj
                FROM {{ ref('politico') }}
                    JOIN {{ source('unibase', 'fornecedor') }} ON politico.cnpj::TEXT = fornecedor.documento::TEXT
            )
        ) AS socio_politico,
        (
            (
                base_supplier_1.cnpj::TEXT,
                base_supplier_1.id_base
            ) IN (
                SELECT grupo_economico.cnpj_empresa_x,
                    grupo_economico.id_base
                FROM conluio.grupo_economico
            )
        ) AS conluio_mesmo_grupo_economico,
        (
            (
                base_supplier_1.cnpj::TEXT,
                base_supplier_1.id_base
            ) IN (
                SELECT telefone.cnpj_empresa_x,
                    telefone.id_base
                FROM conluio.telefone
            )
        ) AS conluio_mesmo_telefone,
        (
            (
                base_supplier_1.cnpj::TEXT,
                base_supplier_1.id_base
            ) IN (
                SELECT socio.cnpj_empresa_x,
                    socio.id_base
                FROM conluio.socio
            )
        ) AS conluio_socio_comum,
        (
            (
                base_supplier_1.cnpj::TEXT,
                base_supplier_1.id_base
            ) IN (
                SELECT top_loser.cnpj_empresa_vencedora,
                    base_supplier_1.id_base
                FROM conluio.top_loser
            )
        ) AS empresa_ganhadora_top_loser,
        (
            (
                base_supplier_1.cnpj::TEXT,
                base_supplier_1.id_base
            ) IN (
                SELECT top_loser.cnpj_empresa_top_loser,
                    base_supplier_1.id_base
                FROM conluio.top_loser
            )
        ) AS empresa_top_loser,
        (
            base_supplier_1.cnpj::TEXT IN (
                SELECT doadora_campanha.cnpj
                FROM {{ ref('doadora_campanha') }}
                    JOIN {{ source('unibase', 'fornecedor') }} ON doadora_campanha.cnpj::TEXT = fornecedor.documento::TEXT
            )
        ) AS doadora_campanha_politica,
        (
            base_supplier_1.cnpj::TEXT IN (
                SELECT doador_campanha.cnpj
                FROM {{ ref('doador_campanha') }}
                    JOIN {{ source('unibase', 'fornecedor') }} ON doador_campanha.cnpj::TEXT = fornecedor.documento::TEXT
            )
        ) AS socio_doador_campanha_politica,
        (
            base_supplier_1.cnpj::TEXT IN (
                SELECT fornecedora_campanha.cnpj
                FROM {{ ref('fornecedora_campanha') }}
                    JOIN {{ source('unibase', 'fornecedor') }} ON fornecedora_campanha.cnpj::TEXT = fornecedor.documento::TEXT
            )
        ) AS fornecedora_campanha_politica,
        (
            base_supplier_1.cnpj::TEXT IN (
                SELECT fornecedor_campanha.cnpj
                FROM {{ ref('fornecedor_campanha') }}
                    JOIN {{ source('unibase', 'fornecedor') }} ON fornecedor_campanha.cnpj::TEXT = fornecedor.documento::TEXT
            )
        ) AS socio_fornecedor_campanha_politica,
        (
            (
                base_supplier_1.cnpj::TEXT,
                base_supplier_1.id_base
            ) IN (
                SELECT propostas_simetricas_absolutas.cnpj_empresa_x,
                    propostas_simetricas_absolutas.id_base
                FROM conluio.propostas_simetricas_absolutas
                WHERE propostas_simetricas_absolutas.diferenca_valor_xy = 0::NUMERIC
            )
        ) AS proposta_identica,
        (
            (
                base_supplier_1.cnpj::TEXT,
                base_supplier_1.id_base
            ) IN (
                SELECT propostas_simetricas_absolutas.cnpj_empresa_x,
                    propostas_simetricas_absolutas.id_base
                FROM conluio.propostas_simetricas_absolutas
                WHERE propostas_simetricas_absolutas.diferenca_valor_xy <> 0::NUMERIC
            )
        ) AS proposta_simetrica_absoluta
    FROM base_supplier base_supplier_1
)
SELECT DISTINCT COALESCE(bool_patterns.proposta_simetrica_absoluta, FALSE) AS proposta_simetrica_absoluta,
    COALESCE(bool_patterns.proposta_identica, FALSE) AS proposta_identica,
    COALESCE(
        bool_patterns.socio_fornecedor_campanha_politica,
        FALSE
    ) AS socio_fornecedor_campanha_politica,
    COALESCE(
        bool_patterns.fornecedora_campanha_politica,
        FALSE
    ) AS fornecedora_campanha_politica,
    COALESCE(
        bool_patterns.socio_doador_campanha_politica,
        FALSE
    ) AS socio_doador_campanha_politica,
    COALESCE(bool_patterns.doadora_campanha_politica, FALSE) AS doadora_campanha_politica,
    FALSE AS socio_baixa_qualificacao,
    FALSE AS socio_baixa_remuneracao,
    COALESCE(bool_patterns.empresa_top_loser, FALSE) AS empresa_top_loser,
    COALESCE(bool_patterns.empresa_ganhadora_top_loser, FALSE) AS empresa_ganhadora_top_loser,
    COALESCE(bool_patterns.conluio_socio_comum, FALSE) AS conluio_socio_comum,
    COALESCE(bool_patterns.conluio_mesmo_telefone, FALSE) AS conluio_mesmo_telefone,
    COALESCE(
        bool_patterns.conluio_mesmo_grupo_economico,
        FALSE
    ) AS conluio_mesmo_grupo_economico,
    FALSE AS conluio_mesmo_endereco,
    COALESCE(bool_patterns.socio_politico, FALSE) AS socio_politico,
    COALESCE(punished_partner.socio_punido_cnep, FALSE) AS socio_punido_cnep,
    COALESCE(punished_partner.socio_punido_cepim, FALSE) AS socio_punido_cepim,
    COALESCE(punished_partner.socio_punido_ceis, FALSE) AS socio_punido_ceis,
    COALESCE(punished_partner.socio_punido_al, FALSE) AS socio_punido_al,
    COALESCE(punished_company.punida_al, FALSE) AS punida_al,
    COALESCE(punished_company.punida_lte, FALSE) AS punida_lte,
    COALESCE(punished_company.punida_cnep, FALSE) AS punida_cnep,
    COALESCE(punished_company.punida_tce_sp, FALSE) AS punida_tce_sp,
    COALESCE(punished_company.punida_cepim, FALSE) AS punida_cepim,
    COALESCE(punished_company.punida_ceis, FALSE) AS punida_ceis,
    COALESCE(
        cct_partner_pattern.socio_beneficiario_bf
        OR cct_partner_pattern.socio_beneficiario_seguro_defeso
        OR cct_partner_pattern.socio_beneficiario_bpc
        OR cct_partner_pattern.socio_beneficiario_garantia_safra
        OR cct_partner_pattern.socio_beneficiario_peti,
        FALSE
    ) AS socio_beneficiario_caso_fraco,
    COALESCE(
        cct_partner_pattern.socio_beneficiario_peti,
        FALSE
    ) AS socio_beneficiario_peti,
    COALESCE(
        cct_partner_pattern.socio_beneficiario_garantia_safra,
        FALSE
    ) AS socio_beneficiario_garantia_safra,
    COALESCE(
        cct_partner_pattern.socio_beneficiario_bpc,
        FALSE
    ) AS socio_beneficiario_bpc,
    COALESCE(
        cct_partner_pattern.socio_beneficiario_seguro_defeso,
        FALSE
    ) AS socio_beneficiario_seguro_defeso,
    COALESCE(cct_partner_pattern.socio_beneficiario_bf, FALSE) AS socio_beneficiario_bf,
    COALESCE(n_competitors.n_licitantes_contra, 0::BIGINT) AS n_licitantes_contra,
    COALESCE(
        n_political_bonds.n_vinculos_partidarios,
        0::BIGINT
    ) AS n_vinculos_partidarios,
    base_supplier.cnpj,
    COALESCE(political_parties.vinculo_partidario, ''::TEXT) AS vinculo_partidario,
    COALESCE(general_info.uf, ''::CHARACTER VARYING) AS uf,
    COALESCE(company_name.nome, ''::CHARACTER VARYING) AS nome,
    COALESCE(
        general_info.data_ultimo_contrato::CHARACTER VARYING,
        ''::CHARACTER VARYING
    ) AS data_ultimo_contrato,
    COALESCE(
        general_info.data_primeiro_contrato::CHARACTER VARYING,
        ''::CHARACTER VARYING
    ) AS data_primeiro_contrato,
    COALESCE(
        general_info.data_ultima_proposta::CHARACTER VARYING,
        ''::CHARACTER VARYING
    ) AS data_ultima_proposta,
    COALESCE(
        general_info.data_primeira_proposta::CHARACTER VARYING,
        ''::CHARACTER VARYING
    ) AS data_primeira_proposta,
    COALESCE(
        general_info.data_constituicao,
        ''::CHARACTER VARYING
    ) AS data_constituicao,
    n_employees.n_funcionarios_2018,
    n_employees.n_funcionarios_2017,
    n_employees.n_funcionarios_2016,
    n_employees.n_funcionarios_2015,
    n_employees.n_funcionarios_2014,
    n_employees.n_funcionarios_2013,
    n_employees.n_funcionarios_2012,
    n_employees.n_funcionarios_2011,
    n_employees.n_funcionarios_2010,
    precocity.precocidade_contrato_em_dias,
    precocity.precocidade_proposta_em_dias,
    COALESCE(n_cnaes.n_cnaes_divisoes, 0::BIGINT) AS n_cnaes_divisoes,
    COALESCE(n_cnaes.n_cnaes_grupos, 0::BIGINT) AS n_cnaes_grupos,
    COALESCE(n_cnaes.n_cnaes_classes, 0::BIGINT) AS n_cnaes_classes,
    COALESCE(n_cnaes.n_cnaes, 0::BIGINT) AS n_cnaes,
    COALESCE(general_info.n_aditivos, 0::BIGINT) AS n_aditivos,
    COALESCE(general_info.n_contratos, 0::BIGINT) AS n_contratos,
    COALESCE(general_info.n_medio_licitantes, 0::NUMERIC) AS n_medio_licitantes,
    COALESCE(
        general_info.n_propostas_ganhadoras_um_licitante,
        0::BIGINT
    ) AS n_propostas_ganhadoras_um_licitante,
    COALESCE(general_info.n_propostas_um_licitante, 0::BIGINT) AS n_propostas_um_licitante,
    COALESCE(general_info.n_propostas_perdedoras, 0::BIGINT) AS n_propostas_perdedoras,
    COALESCE(general_info.n_propostas_ganhadoras, 0::BIGINT) AS n_propostas_ganhadoras,
    COALESCE(general_info.n_licitacoes, 0::BIGINT) AS n_licitacoes,
    COALESCE(general_info.n_propostas, 0::BIGINT) AS n_propostas,
    COALESCE(general_info.n_socios, 0::BIGINT) AS n_socios,
    base_supplier.id_fornecedor,
    base_supplier.id_base,
    COALESCE(
        general_info.majoracao_contratual_via_aditivos,
        0::DOUBLE PRECISION
    ) AS majoracao_contratual_via_aditivos,
    COALESCE(
        general_info.valor_total_aditivos,
        0::DOUBLE PRECISION
    ) AS valor_total_aditivos,
    COALESCE(
        general_info.valor_total_contratos,
        0::DOUBLE PRECISION
    ) AS valor_total_contratos,
    COALESCE(general_info.taxa_vitorias, 0::NUMERIC) AS taxa_vitorias,
    COALESCE(
        general_info.valor_total_propostas_ganhadoras,
        0::DOUBLE PRECISION
    ) AS valor_total_propostas_ganhadoras,
    COALESCE(
        general_info.valor_total_propostas_ganhadoras_um_licitante,
        0::DOUBLE PRECISION
    ) AS valor_total_propostas_ganhadoras_um_licitante,
    COALESCE(
        general_info.valor_total_propostas_um_licitante,
        0::DOUBLE PRECISION
    ) AS valor_total_propostas_um_licitante,
    COALESCE(
        general_info.valor_total_propostas,
        0::DOUBLE PRECISION
    ) AS valor_total_propostas,
    COALESCE(general_info.capital_social, 0::DOUBLE PRECISION) AS capital_social
FROM base_supplier
    LEFT JOIN company_name ON base_supplier.cnpj::bpchar = company_name.cnpj
    LEFT JOIN general_info ON base_supplier.id_base = general_info.id_base
    AND base_supplier.id_fornecedor = general_info.id_fornecedor
    LEFT JOIN cct_partner_pattern ON base_supplier.cnpj::TEXT = cct_partner_pattern.cnpj::TEXT
    LEFT JOIN punished_company ON punished_company.cnpj::TEXT = base_supplier.cnpj::TEXT
    LEFT JOIN punished_partner ON base_supplier.cnpj::TEXT = punished_partner.cnpj::TEXT
    LEFT JOIN n_cnaes ON base_supplier.cnpj::TEXT = n_cnaes.cnpj::TEXT
    LEFT JOIN precocity ON base_supplier.cnpj::TEXT = precocity.cnpj::TEXT
    AND base_supplier.id_base = precocity.id_base
    LEFT JOIN n_employees ON base_supplier.cnpj::TEXT = n_employees.cnpj::TEXT
    LEFT JOIN n_political_bonds ON base_supplier.cnpj::TEXT = n_political_bonds.cnpj::TEXT
    LEFT JOIN political_parties ON base_supplier.cnpj::TEXT = political_parties.cnpj::TEXT
    LEFT JOIN n_competitors ON base_supplier.id_fornecedor = n_competitors.id_fornecedor
    AND base_supplier.id_base = n_competitors.id_base
    LEFT JOIN bool_patterns ON base_supplier.cnpj::TEXT = bool_patterns.cnpj::TEXT
    AND base_supplier.id_base = bool_patterns.id_base