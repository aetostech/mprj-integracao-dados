WITH high_earning_companies AS (
    SELECT p.id_fornecedor
    FROM {{ source('unibase', 'proposta') }} p
        JOIN {{ source('unibase', 'lote') }} l ON p.id_lote = l.id_lote
    WHERE l.num_propostas <={{ var('conluio_socio_num_propostas_nao_competitivo_max') }}
        AND p.id_avoid = 0
        AND p.proposta_ganhadora
    GROUP BY p.id_fornecedor
    HAVING SUM(p.valor) >{{ var('conluio_socio_valor_ganho_conluio_min') }}
        AND COUNT(DISTINCT l.id_lote) >={{ var('conluio_socio_num_lotes_juntas_min') }}
),
non_competitive_biddings AS (
    SELECT p.id_proposta,
        p.quantidade,
        p.valor_unitario,
        p.valor,
        p.unidade,
        p.data,
        p.proposta_ganhadora,
        p.id_lote,
        p.id_fornecedor,
        p.id_base,
        p.id_avoid
    FROM {{ source('unibase', 'proposta') }} p
        JOIN {{ source('unibase', 'lote') }} l ON p.id_lote = l.id_lote
    WHERE l.num_propostas <={{ var('conluio_socio_num_propostas_nao_competitivo_max') }}
),
unibase_companies_partners AS (
    SELECT s.cnpj,
        s.documento_socio,
        s.nome_socio,
        s.data_entrada_sociedade,
        s.data_saida_sociedade,
        s.origem
    FROM {{ source('dados_cadastrais', 'socio') }} s
    WHERE (
            s.cnpj::TEXT IN (
                SELECT fornecedor.documento
                FROM {{ source('unibase', 'fornecedor') }}
            )
        )
),
same_partner AS (
    SELECT s1.cnpj AS cnpj_empresa_x,
        s2.cnpj AS cnpj_empresa_y,
        s1.documento_socio AS cpf_socio
    FROM high_earning_companies hec
        JOIN {{ source('unibase', 'fornecedor') }} f1 ON f1.id_fornecedor = hec.id_fornecedor
        JOIN unibase_companies_partners s1 ON s1.cnpj::TEXT = f1.documento::TEXT
        JOIN unibase_companies_partners s2 ON s1.documento_socio::TEXT = s2.documento_socio::TEXT
        AND s1.cnpj::TEXT <> s2.cnpj::TEXT
        AND s1.nome_socio::TEXT = s2.nome_socio::TEXT
        JOIN {{ source('unibase', 'fornecedor') }} f2 ON s2.cnpj::TEXT = f2.documento::TEXT
    WHERE LENGTH(s1.documento_socio::TEXT) = 11
)
SELECT px.id_base,
    same_partner.cnpj_empresa_x,
    same_partner.cnpj_empresa_y,
    same_partner.cpf_socio,
    SUM(
        CASE
            WHEN px.proposta_ganhadora THEN px.valor
            ELSE 0::DOUBLE PRECISION
        END
    ) AS valor_propostas_ganhadoras_x,
    SUM(
        CASE
            WHEN px.proposta_ganhadora THEN py.valor
            ELSE 0::DOUBLE PRECISION
        END
    ) AS valor_propostas_ganhadoras_y,
    COUNT(DISTINCT px.id_lote) AS n_lotes_juntos,
    SUM(px.proposta_ganhadora::INTEGER) AS n_x_ganhou,
    SUM(py.proposta_ganhadora::INTEGER) AS n_y_ganhou,
    SUM(px.valor) AS valor_total_propostas_x,
    SUM(py.valor) AS valor_total_propostas_y,
    SUM(px.proposta_ganhadora::INTEGER)::DOUBLE PRECISION / COUNT(DISTINCT px.id_lote)::DOUBLE PRECISION AS frequencia_vitorias_x,
    SUM(py.proposta_ganhadora::INTEGER)::DOUBLE PRECISION / COUNT(DISTINCT px.id_lote)::DOUBLE PRECISION AS frequencia_vitorias_y,
    SUM(
        CASE
            WHEN px.proposta_ganhadora THEN px.valor
            ELSE 0::DOUBLE PRECISION
        END
    ) / SUM(px.valor) AS significancia_vitorias_x,
    SUM(
        CASE
            WHEN py.proposta_ganhadora THEN py.valor
            ELSE 0::DOUBLE PRECISION
        END
    ) / SUM(py.valor) AS significancia_vitorias_y
FROM same_partner
    JOIN {{ source('unibase', 'fornecedor') }} fx ON same_partner.cnpj_empresa_x::TEXT = fx.documento::TEXT
    JOIN {{ source('unibase', 'fornecedor') }} fy ON same_partner.cnpj_empresa_y::TEXT = fy.documento::TEXT
    JOIN non_competitive_biddings px ON fx.id_fornecedor = px.id_fornecedor
    JOIN non_competitive_biddings py ON fy.id_fornecedor = py.id_fornecedor
WHERE px.id_lote = py.id_lote
    AND px.id_avoid = 0
    AND py.id_avoid = 0
    AND "substring"(same_partner.cnpj_empresa_x::TEXT, 1, 8) <> "substring"(same_partner.cnpj_empresa_y::TEXT, 1, 8)
GROUP BY px.id_base,
    same_partner.cnpj_empresa_x,
    same_partner.cnpj_empresa_y,
    same_partner.cpf_socio
HAVING SUM(
        CASE
            WHEN px.proposta_ganhadora THEN px.valor
            ELSE 0::DOUBLE PRECISION
        END
    ) >={{ var('conluio_socio_valor_ganho_conluio_min') }}
    AND COUNT(DISTINCT px.id_lote) >={{ var('conluio_socio_num_lotes_juntas_min') }}