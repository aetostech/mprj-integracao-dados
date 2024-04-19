WITH dados_contratos AS (
    SELECT c.id_base,
        e.id_orgao_superior,
        SUM(c.valor) AS valor_total_contratos,
        COUNT(DISTINCT c.id_contrato) AS n_contratos
    FROM {{ source('unibase', 'contrato') }} c
        JOIN {{ source('unibase', 'entidade') }} e ON c.id_entidade = e.id_entidade
        AND c.id_base = e.id_base
    WHERE c.id_avoid = 0
    GROUP BY e.id_orgao_superior,
        c.id_base
),
dados_propostas AS (
    SELECT l.id_base,
        e.id_orgao_superior,
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
    GROUP BY e.id_orgao_superior,
        l.id_base
)
SELECT o.id_base,
    o.id_orgao_superior,
    o.nome AS nome_orgao_superior,
    COALESCE(dc.n_contratos, 0::BIGINT) AS n_contratos,
    COALESCE(dc.valor_total_contratos, 0::DOUBLE PRECISION) AS valor_total_contratos,
    COALESCE(dp.n_propostas, 0::BIGINT) AS n_propostas,
    COALESCE(dp.valor_total_propostas, 0::DOUBLE PRECISION) AS valor_total_propostas,
    COALESCE(dp.n_propostas_ganhadoras, 0::BIGINT) AS n_propostas_ganhadoras,
    COALESCE(
        dp.valor_total_propostas_ganhadoras,
        0::DOUBLE PRECISION
    ) AS valor_total_propostas_ganhadoras
FROM {{ source('unibase', 'orgao_superior') }} o
    JOIN dados_contratos dc ON o.id_orgao_superior = dc.id_orgao_superior
    AND o.id_base = dc.id_base
    JOIN dados_propostas dp ON o.id_orgao_superior = dp.id_orgao_superior
    AND o.id_base = dp.id_base
WHERE dc.n_contratos > 0
    OR dp.n_propostas > 0