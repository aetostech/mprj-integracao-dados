WITH dados_contratos AS (
    SELECT contrato.id_base,
        contrato.id_entidade,
        SUM(contrato.valor) AS valor_total_contratos,
        COUNT(DISTINCT contrato.id_contrato) AS n_contratos
    FROM {{ source('unibase', 'contrato') }}
    WHERE contrato.id_avoid = 0
    GROUP BY contrato.id_entidade,
        contrato.id_base
),
dados_propostas AS (
    SELECT l.id_base,
        l.id_entidade,
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
    WHERE p.id_avoid = 0
    GROUP BY l.id_entidade,
        l.id_base
),
dados_gerais_propostas AS (
    SELECT l.id_entidade,
        COUNT(DISTINCT l.id_lote) AS n_lotes,
        COUNT(DISTINCT p.id_fornecedor) AS n_licitantes,
        COUNT(
            DISTINCT CASE
                WHEN p.proposta_ganhadora THEN p.id_fornecedor
                ELSE NULL::INTEGER
            END
        ) AS n_licitantes_ganhadores
    FROM {{ source('unibase', 'lote') }} l
        JOIN {{ source('unibase', 'proposta') }} p ON l.id_lote = p.id_lote
    WHERE p.id_avoid = 0
    GROUP BY l.id_entidade
),
dados_gerais_contratos AS (
    SELECT contrato.id_entidade,
        COUNT(DISTINCT contrato.id_fornecedor) AS n_contratados
    FROM {{ source('unibase', 'contrato') }}
    WHERE contrato.id_avoid = 0
    GROUP BY contrato.id_entidade
)
SELECT e.id_base,
    e.id_entidade,
    e.nome AS nome_entidade,
    COALESCE(dc.n_contratos, 0::BIGINT) AS n_contratos,
    COALESCE(dc.valor_total_contratos, 0::DOUBLE PRECISION) AS valor_total_contratos,
    COALESCE(dp.n_propostas, 0::BIGINT) AS n_propostas,
    COALESCE(dp.valor_total_propostas, 0::DOUBLE PRECISION) AS valor_total_propostas,
    COALESCE(dp.n_propostas_ganhadoras, 0::BIGINT) AS n_propostas_ganhadoras,
    COALESCE(
        dp.valor_total_propostas_ganhadoras,
        0::DOUBLE PRECISION
    ) AS valor_total_propostas_ganhadoras,
    COALESCE(dgp.n_lotes, 0::BIGINT) AS n_lotes,
    COALESCE(dgp.n_licitantes, 0::BIGINT) AS n_licitantes,
    COALESCE(dgp.n_licitantes_ganhadores, 0::BIGINT) AS n_licitantes_ganhadores,
    COALESCE(dgc.n_contratados, 0::BIGINT) AS n_contratados
FROM {{ source('unibase', 'entidade') }} e
    LEFT JOIN dados_contratos dc ON e.id_entidade = dc.id_entidade
    AND e.id_base = dc.id_base
    LEFT JOIN dados_propostas dp ON e.id_entidade = dp.id_entidade
    AND e.id_base = dp.id_base
    LEFT JOIN dados_gerais_propostas dgp ON e.id_entidade = dgp.id_entidade
    LEFT JOIN dados_gerais_contratos dgc ON e.id_entidade = dgc.id_entidade
WHERE dc.n_contratos > 0
    OR dp.n_propostas > 0