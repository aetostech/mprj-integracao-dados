WITH contracts_grouped AS (
    SELECT contrato.id_base,
        contrato.id_entidade,
        contrato.id_fornecedor,
        SUM(contrato.valor) AS valor_total_contratos,
        COUNT(*) AS n_contratos
    FROM {{ source('unibase', 'contrato') }}
    WHERE contrato.id_avoid = 0
        AND contrato.valor IS NOT NULL
        AND contrato.valor <> 0::DOUBLE PRECISION
    GROUP BY contrato.id_base,
        contrato.id_entidade,
        contrato.id_fornecedor
),
additives_grouped AS (
    SELECT a.id_base,
        c.id_entidade,
        c.id_fornecedor,
        SUM(a.valor) AS valor_total_aditivos,
        COUNT(*) AS n_aditivos
    FROM {{ source('unibase', 'aditivo') }} a
        JOIN (
            SELECT DISTINCT contrato.id_contrato,
                contrato.id_entidade,
                contrato.id_fornecedor,
                contrato.id_base
            FROM {{ source('unibase', 'contrato') }}
        ) c ON a.id_contrato = c.id_contrato
        AND a.id_base = c.id_base
    WHERE a.id_avoid = 0
        AND a.valor IS NOT NULL
        AND a.valor <> 0::DOUBLE PRECISION
    GROUP BY a.id_base,
        c.id_entidade,
        c.id_fornecedor
)
SELECT cg.id_base,
    cg.id_entidade,
    entidade.nome AS nome_entidade,
    f.documento AS cnpj,
    ag.valor_total_aditivos / cg.valor_total_contratos AS significancia_aditivos,
    ag.n_aditivos,
    ag.valor_total_aditivos,
    cg.valor_total_contratos
FROM contracts_grouped cg
    JOIN additives_grouped ag ON cg.id_entidade = ag.id_entidade
    AND cg.id_fornecedor = ag.id_fornecedor
    AND cg.id_base = ag.id_base
    JOIN {{ source('unibase', 'fornecedor') }} f ON cg.id_fornecedor = f.id_fornecedor
    JOIN {{ source('unibase', 'entidade') }} ON cg.id_entidade = entidade.id_entidade
WHERE (
        ag.valor_total_aditivos / cg.valor_total_contratos
    ) > {{ var('majoracao_aditivos_taxa_min') }}