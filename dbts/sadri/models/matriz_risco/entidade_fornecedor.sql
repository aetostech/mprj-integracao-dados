WITH dados_gerais_contrato AS (
    SELECT e.id_base,
        e.id_entidade,
        MAX(e.nome::TEXT) AS nome_entidade,
        c.id_fornecedor,
        SUM(c.valor) AS valor_total_contratos,
        COUNT(DISTINCT c.id_contrato) AS n_contratos
    FROM {{ source('unibase', 'contrato') }} c
        JOIN {{ source('unibase', 'entidade') }} e ON c.id_entidade = e.id_entidade
        AND c.id_base = e.id_base
    WHERE c.id_avoid = 0
    GROUP BY e.id_base,
        e.id_entidade,
        c.id_fornecedor
),
dados_gerais_proposta AS (
    SELECT l.id_base,
        e.id_entidade,
        p.id_fornecedor,
        MAX(e.nome::TEXT) AS nome_entidade,
        SUM(p.valor) AS valor_total_propostas,
        COUNT(DISTINCT p.id_proposta) AS n_propostas,
        SUM(
            CASE
                WHEN p.proposta_ganhadora THEN p.valor
                ELSE 0::DOUBLE PRECISION
            END
        ) AS valor_total_propostas_ganhadoras,
        SUM(p.proposta_ganhadora::INTEGER) AS n_propostas_ganhadoras
    FROM {{ source('unibase', 'proposta') }} p
        JOIN {{ source('unibase', 'lote') }} l ON p.id_lote = l.id_lote
        JOIN {{ source('unibase', 'entidade') }} e ON e.id_entidade = l.id_entidade
    WHERE p.id_avoid = 0
    GROUP BY l.id_base,
        e.id_entidade,
        p.id_fornecedor
),
numero_socios_empresa AS (
    SELECT f_1.id_fornecedor,
        COUNT(*) AS n_socios
    FROM {{ source('unibase', 'fornecedor') }} f_1
        JOIN (
            SELECT socio.cnpj,
                socio.documento_socio
            FROM {{ source('dados_cadastrais', 'socio') }}
        ) s ON f_1.documento::TEXT = s.cnpj::TEXT
    GROUP BY f_1.id_fornecedor
),
socio_politico_n AS (
    SELECT politico.cnpj,
        COUNT(DISTINCT politico.cpf) AS n_socios_politicos
    FROM {{ ref('politico') }}
    GROUP BY politico.cnpj
),
punicoes_n AS (
    SELECT punicao.documento AS cnpj,
        COUNT(*) AS n_punicoes
    FROM {{ source('dados_cadastrais', 'punicao') }}
    GROUP BY punicao.documento
),
punicoes_socios_n AS (
    SELECT punido.cnpj,
        COUNT(DISTINCT punido.cpf) AS n_socios_punidos
    FROM {{ ref('punido') }}
    GROUP BY punido.cnpj
),
socios_beneficiarios_n AS (
    SELECT beneficiario.cnpj,
        COUNT(DISTINCT beneficiario.cpf_socio_beneficiario) AS n_socios_beneficiarios
    FROM {{ ref('beneficiario') }}
    GROUP BY beneficiario.cnpj
),
entidade_fornecedor AS (
    SELECT CASE
            WHEN dgc.id_fornecedor IS NULL THEN dgp.id_fornecedor
            ELSE dgc.id_fornecedor
        END AS id_fornecedor,
        CASE
            WHEN dgc.id_entidade IS NULL THEN dgp.id_entidade
            ELSE dgc.id_entidade
        END AS id_entidade,
        CASE
            WHEN dgc.nome_entidade IS NULL THEN dgp.nome_entidade
            ELSE dgc.nome_entidade
        END AS nome_entidade,
        CASE
            WHEN dgc.id_base IS NULL THEN dgp.id_base
            ELSE dgc.id_base
        END AS id_base,
        COALESCE(dgc.n_contratos) AS n_contratos,
        COALESCE(dgp.n_propostas) AS n_propostas,
        COALESCE(dgc.valor_total_contratos) AS valor_total_contratos,
        COALESCE(dgp.valor_total_propostas) AS valor_total_propostas,
        COALESCE(dgp.valor_total_propostas_ganhadoras) AS valor_total_propostas_ganhadoras,
        COALESCE(dgp.n_propostas_ganhadoras) AS n_propostas_ganhadoras
    FROM dados_gerais_contrato dgc
        FULL JOIN dados_gerais_proposta dgp ON dgc.id_entidade = dgp.id_entidade
        AND dgc.id_fornecedor = dgp.id_fornecedor
        AND dgc.id_base = dgp.id_base
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
)
SELECT DISTINCT ef.id_base,
    ef.id_entidade,
    ef.nome_entidade,
    COALESCE(ef.valor_total_contratos) AS valor_total_contratos,
    COALESCE(ef.n_contratos) AS n_contratos,
    COALESCE(ef.valor_total_propostas) AS valor_total_propostas,
    COALESCE(ef.n_propostas) AS n_propostas,
    COALESCE(ef.valor_total_propostas_ganhadoras) AS valor_total_propostas_ganhadoras,
    COALESCE(ef.n_propostas_ganhadoras) AS n_propostas_ganhadoras,
    COALESCE(f.documento) AS cnpj_empresa,
    f.nome AS nome_empresa,
    COALESCE(nse.n_socios) AS n_socios,
    COALESCE(spn.n_socios_politicos) AS n_socios_politicos,
    COALESCE(pjnc.n_cnaes) AS n_cnaes,
    COALESCE(pjnc.n_cnaes_classes) AS n_cnaes_classes,
    COALESCE(pjnc.n_cnaes_grupos) AS n_cnaes_grupos,
    COALESCE(pjnc.n_cnaes_divisoes) AS n_cnaes_divisoes,
    COALESCE(pn.n_punicoes) AS n_punicoes,
    COALESCE(psn.n_socios_punidos) AS n_socios_punidos,
    COALESCE(sbn.n_socios_beneficiarios) AS n_socios_beneficiarios
FROM entidade_fornecedor ef
    LEFT JOIN {{ source('unibase', 'fornecedor') }} f ON f.id_fornecedor = ef.id_fornecedor
    LEFT JOIN numero_socios_empresa nse ON ef.id_fornecedor = nse.id_fornecedor
    LEFT JOIN socio_politico_n spn ON f.documento::TEXT = spn.cnpj::TEXT
    LEFT JOIN n_cnaes pjnc ON f.documento::TEXT = pjnc.cnpj::TEXT
    LEFT JOIN punicoes_n pn ON f.documento::TEXT = pn.cnpj::TEXT
    LEFT JOIN punicoes_socios_n psn ON f.documento::TEXT = psn.cnpj::TEXT
    LEFT JOIN socios_beneficiarios_n sbn ON f.documento::TEXT = sbn.cnpj::TEXT