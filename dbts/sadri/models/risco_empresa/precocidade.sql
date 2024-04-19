WITH precocidade_proposta AS (
    SELECT p.id_base,
        p.id_fornecedor,
        MIN(p.data) AS data_primeira_proposta
    FROM {{ source('unibase', 'proposta') }} p
    WHERE p.id_avoid = 0
        AND p.data IS NOT NULL
    GROUP BY p.id_base,
        p.id_fornecedor
),
precocidade_contrato AS (
    SELECT c_1.id_base,
        c_1.id_fornecedor,
        MIN(c_1.data) AS data_primeiro_contrato
    FROM {{ source('unibase', 'contrato') }} c_1
    WHERE c_1.id_avoid = 0
        AND c_1.data IS NOT NULL
    GROUP BY c_1.id_base,
        c_1.id_fornecedor
),
constituicao AS (
    SELECT pessoa_juridica.cnpj,
        pessoa_juridica.data_inicio_atividade AS data_constituicao
    FROM {{ source('dados_cadastrais', 'pessoa_juridica') }}
)
SELECT fb.id_base,
    fb.id_fornecedor,
    f.documento AS cnpj,
    pp.data_primeira_proposta,
    pc.data_primeiro_contrato,
    c.data_constituicao,
    pp.data_primeira_proposta - TO_DATE(c.data_constituicao::TEXT, 'YYYY-MM-DD'::TEXT) AS dias_ate_proposta,
    pc.data_primeiro_contrato - TO_DATE(c.data_constituicao::TEXT, 'YYYY-MM-DD'::TEXT) AS dias_ate_contrato
FROM (
        SELECT DISTINCT contrato.id_fornecedor,
            contrato.id_base
        FROM {{ source('unibase', 'contrato') }}
        UNION
        SELECT DISTINCT proposta.id_fornecedor,
            proposta.id_base
        FROM {{ source('unibase', 'proposta') }}
    ) fb
    JOIN {{ source('unibase', 'fornecedor') }} f ON fb.id_fornecedor = f.id_fornecedor
    LEFT JOIN precocidade_proposta pp ON fb.id_fornecedor = pp.id_fornecedor
    AND fb.id_base = pp.id_base
    LEFT JOIN precocidade_contrato pc ON fb.id_fornecedor = pc.id_fornecedor
    AND fb.id_base = pc.id_base
    LEFT JOIN constituicao c ON f.documento::bpchar = c.cnpj