WITH dados_gerais_contrato AS (
    SELECT e.id_base,
        e.id_orgao_superior,
        MAX(os.nome::TEXT) AS nome_orgao_superior,
        c.id_fornecedor,
        SUM(c.valor) AS valor_total_contratos,
        COUNT(DISTINCT c.id_contrato) AS n_contratos
    FROM {{ source('unibase', 'contrato') }} c
        JOIN {{ source('unibase', 'entidade') }} e ON c.id_entidade = e.id_entidade
        AND c.id_base = e.id_base
        JOIN {{ source('unibase', 'orgao_superior') }} os ON e.id_orgao_superior = os.id_orgao_superior
        AND e.id_base = os.id_base
    WHERE c.id_avoid = 0
    GROUP BY e.id_base,
        e.id_orgao_superior,
        c.id_fornecedor
),
dados_gerais_proposta AS (
    SELECT l.id_base,
        os.id_orgao_superior,
        p.id_fornecedor,
        MAX(os.nome::TEXT) AS nome_orgao_superior,
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
        AND p.id_base = l.id_base
        JOIN {{ source('unibase', 'entidade') }} e ON l.id_entidade = e.id_entidade
        AND l.id_base = e.id_base
        JOIN {{ source('unibase', 'orgao_superior') }} os ON e.id_orgao_superior = os.id_orgao_superior
    WHERE p.id_avoid = 0
    GROUP BY l.id_base,
        os.id_orgao_superior,
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
orgao_superior_fornecedor AS (
    SELECT CASE
            WHEN dgc.id_fornecedor IS NULL THEN dgp.id_fornecedor
            ELSE dgc.id_fornecedor
        END AS id_fornecedor,
        CASE
            WHEN dgc.id_orgao_superior IS NULL THEN dgp.id_orgao_superior
            ELSE dgc.id_orgao_superior
        END AS id_orgao_superior,
        CASE
            WHEN dgc.nome_orgao_superior IS NULL THEN dgp.nome_orgao_superior
            ELSE dgc.nome_orgao_superior
        END AS nome_orgao_superior,
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
        FULL JOIN dados_gerais_proposta dgp ON dgc.id_orgao_superior = dgp.id_orgao_superior
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
SELECT osf.id_base,
    osf.id_orgao_superior,
    osf.nome_orgao_superior,
    f.documento AS cnpj_empresa,
    f.nome AS nome_empresa,
    osf.n_contratos,
    osf.valor_total_contratos,
    osf.n_propostas,
    osf.valor_total_propostas,
    osf.n_propostas_ganhadoras,
    osf.valor_total_propostas_ganhadoras,
    nse.n_socios,
    spn.n_socios_politicos,
    pjnc.n_cnaes,
    pjnc.n_cnaes_classes,
    pjnc.n_cnaes_grupos,
    pjnc.n_cnaes_divisoes,
    pn.n_punicoes,
    psn.n_socios_punidos,
    sbn.n_socios_beneficiarios
FROM orgao_superior_fornecedor osf
    LEFT JOIN {{ source('unibase', 'fornecedor') }} f ON f.id_fornecedor = osf.id_fornecedor
    LEFT JOIN numero_socios_empresa nse ON osf.id_fornecedor = nse.id_fornecedor
    LEFT JOIN socio_politico_n spn ON f.documento::TEXT = spn.cnpj::TEXT
    LEFT JOIN n_cnaes pjnc ON f.documento::TEXT = pjnc.cnpj::TEXT
    LEFT JOIN punicoes_n pn ON f.documento::TEXT = pn.cnpj::TEXT
    LEFT JOIN punicoes_socios_n psn ON f.documento::TEXT = psn.cnpj::TEXT
    LEFT JOIN socios_beneficiarios_n sbn ON f.documento::TEXT = sbn.cnpj::TEXT
WHERE osf.valor_total_contratos > 0::DOUBLE PRECISION