WITH fornecedor_base AS (
    SELECT DISTINCT contrato.id_fornecedor,
        contrato.id_base
    FROM {{ source('unibase', 'contrato') }}
    UNION
    SELECT DISTINCT proposta.id_fornecedor,
        proposta.id_base
    FROM {{ source('unibase', 'proposta') }}
)
SELECT ROW_NUMBER() OVER (
        ORDER BY (
                (
                    SELECT 1
                )
            )
    ) AS id_politico,
    politicians.id_base,
    politicians.cpf_politico,
    politicians.nome_politico,
    politicians.partido,
    politicians.cargo,
    politicians.ano,
    politicians.eleito,
    politican_partner.n_empresas_contratadas,
    politican_partner.valor_total_contratos,
    donations.valor_total_doacao,
    suppliers.valor_total_despesa,
    risk_companies.n_empresas_risco
FROM (
        SELECT base.id_base,
            candidatura.cpf AS cpf_politico,
            MAX(candidatura.nome::TEXT) AS nome_politico,
            STRING_AGG(
                DISTINCT candidatura.partido::TEXT,
                '; '::CHARACTER VARYING::TEXT
            ) AS partido,
            STRING_AGG(
                DISTINCT candidatura.cargo::TEXT,
                '; '::CHARACTER VARYING::TEXT
            ) AS cargo,
            STRING_AGG(
                DISTINCT candidatura.ano::CHARACTER VARYING::TEXT,
                '; '::CHARACTER VARYING::TEXT
            ) AS ano,
            BOOL_OR(candidatura.eleito) AS eleito
        FROM {{ source('dados_cadastrais', 'candidatura') }}
            CROSS JOIN {{ source('unibase', 'base') }}
        GROUP BY candidatura.cpf,
            base.id_base
    ) politicians
    LEFT JOIN (
        SELECT contrato.id_base,
            politico.cpf AS cpf_politico,
            COUNT(DISTINCT politico.cnpj) AS n_empresas_contratadas,
            SUM(contrato.valor) AS valor_total_contratos
        FROM {{ ref('politico') }}
            JOIN {{ source('unibase', 'fornecedor') }} ON politico.cnpj::TEXT = fornecedor.documento::TEXT
            JOIN {{ source('unibase', 'contrato') }} ON fornecedor.id_fornecedor = contrato.id_fornecedor
        WHERE contrato.id_avoid = 0
        GROUP BY politico.cpf,
            contrato.id_base
    ) politican_partner ON politicians.cpf_politico::TEXT = politican_partner.cpf_politico::TEXT
    AND politicians.id_base = politican_partner.id_base
    LEFT JOIN (
        SELECT t.id_base,
            t.cpf_politico,
            SUM(t.valor_total_doacao) AS valor_total_doacao
        FROM (
                SELECT fornecedor_base.id_base,
                    dce.cpf_candidato AS cpf_politico,
                    dce.valor_doado AS valor_total_doacao
                FROM {{ ref('doadora_campanha') }} dce
                    JOIN {{ source('unibase', 'fornecedor') }} ON dce.cnpj::TEXT = fornecedor.documento::TEXT
                    JOIN fornecedor_base ON fornecedor.id_fornecedor = fornecedor_base.id_fornecedor
                UNION ALL
                SELECT fornecedor_base.id_base,
                    dcs.cpf_candidato AS cpf_politico,
                    dcs.valor_doado AS valor_total_doacao
                FROM {{ ref('doador_campanha') }} dcs
                    JOIN {{ source('unibase', 'fornecedor') }} ON dcs.cnpj::TEXT = fornecedor.documento::TEXT
                    JOIN fornecedor_base ON fornecedor.id_fornecedor = fornecedor_base.id_fornecedor
            ) t
        GROUP BY t.id_base,
            t.cpf_politico
    ) donations ON politicians.cpf_politico::TEXT = donations.cpf_politico::TEXT
    AND politicians.id_base = donations.id_base
    LEFT JOIN (
        SELECT t.id_base,
            t.cpf_politico,
            SUM(t.valor_total_despesa) AS valor_total_despesa
        FROM (
                SELECT fornecedor_base.id_base,
                    fce.cpf_candidato AS cpf_politico,
                    fce.valor_despesa AS valor_total_despesa
                FROM {{ ref('fornecedora_campanha') }} fce
                    JOIN {{ source('unibase', 'fornecedor') }} ON fce.cnpj::TEXT = fornecedor.documento::TEXT
                    JOIN fornecedor_base ON fornecedor.id_fornecedor = fornecedor_base.id_fornecedor
                UNION ALL
                SELECT fornecedor_base.id_base,
                    fcs.cpf_candidato AS cpf_politico,
                    fcs.valor_despesa AS valor_total_despesa
                FROM {{ ref('fornecedor_campanha') }} fcs
                    JOIN {{ source('unibase', 'fornecedor') }} ON fcs.cnpj::TEXT = fornecedor.documento::TEXT
                    JOIN fornecedor_base ON fornecedor.id_fornecedor = fornecedor_base.id_fornecedor
            ) t
        GROUP BY t.id_base,
            t.cpf_politico
    ) suppliers ON politicians.cpf_politico::TEXT = suppliers.cpf_politico::TEXT
    AND politicians.id_base = suppliers.id_base
    LEFT JOIN (
        SELECT risco_rede_politico.id_base,
            risco_rede_politico.cpf_politico,
            COUNT(DISTINCT risco_rede_politico.cnpj) AS n_empresas_risco
        FROM {{ ref('risco_rede_politico') }}
        GROUP BY risco_rede_politico.cpf_politico,
            risco_rede_politico.id_base
    ) risk_companies ON politicians.cpf_politico::TEXT = risk_companies.cpf_politico::TEXT
    AND politicians.id_base = risk_companies.id_base
WHERE politican_partner.valor_total_contratos > 0::DOUBLE PRECISION