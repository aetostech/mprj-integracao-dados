WITH same_telephone AS (
    SELECT f1.id_fornecedor AS id_fornecedor_x,
        f1.documento AS cnpj_empresa_x,
        f1.nome AS nome_empresa_x,
        f2.id_fornecedor AS id_fornecedor_y,
        f2.documento AS cnpj_empresa_y,
        f2.nome AS nome_empresa_y,
        pj1.telefone
    FROM {{ source('unibase', 'fornecedor') }} f1
        JOIN {{ source('dados_cadastrais', 'pessoa_juridica') }} pj1 ON f1.documento::bpchar = pj1.cnpj
        JOIN {{ source('dados_cadastrais', 'pessoa_juridica') }} pj2 ON pj1.telefone::TEXT = pj2.telefone::TEXT
        JOIN {{ source('unibase', 'fornecedor') }} f2 ON f2.documento::bpchar = pj2.cnpj
    WHERE pj1.telefone::TEXT <> ''::TEXT
)
SELECT st.telefone,
    pp.id_base,
    st.cnpj_empresa_x,
    st.cnpj_empresa_y,
    st.nome_empresa_x,
    st.nome_empresa_y,
    pp.n_lotes_juntos,
    pp.n_x_ganhou,
    pp.n_y_ganhou,
    pp.frequencia_vitorias_x,
    pp.frequencia_vitorias_y,
    pp.significancia_vitorias_x,
    pp.significancia_vitorias_y,
    pp.valor_total_propostas_x,
    pp.valor_total_propostas_y
FROM (
        WITH propostas_nao_competitivas AS (
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
            WHERE l.num_propostas >= 2
                AND l.num_propostas <={{ var('conluio_telefone_num_propostas_nao_competitivo_max') }}
        )
        SELECT px.id_base,
            px.id_fornecedor AS id_fornecedor_x,
            py.id_fornecedor AS id_fornecedor_y,
            COUNT(px.id_lote) AS n_lotes_juntos,
            SUM(px.proposta_ganhadora::INTEGER) AS n_x_ganhou,
            SUM(py.proposta_ganhadora::INTEGER) AS n_y_ganhou,
            SUM(px.proposta_ganhadora::INTEGER)::DOUBLE PRECISION / NULLIF(COUNT(px.id_lote), 0) AS frequencia_vitorias_x,
            SUM(py.proposta_ganhadora::INTEGER)::DOUBLE PRECISION / NULLIF(COUNT(px.id_lote), 0) AS frequencia_vitorias_y,
            SUM(
                CASE
                    WHEN px.proposta_ganhadora THEN px.valor
                    ELSE 0::DOUBLE PRECISION
                END
            ) / NULLIF(SUM(px.valor), 0) AS significancia_vitorias_x,
            SUM(
                CASE
                    WHEN py.proposta_ganhadora THEN py.valor
                    ELSE 0::DOUBLE PRECISION
                END
            ) / NULLIF(SUM(py.valor), 0) AS significancia_vitorias_y,
            SUM(px.valor) AS valor_total_propostas_x,
            SUM(py.valor) AS valor_total_propostas_y
        FROM propostas_nao_competitivas px
            JOIN propostas_nao_competitivas py ON px.id_lote = py.id_lote
        WHERE px.valor IS NOT NULL
            AND px.id_fornecedor <> py.id_fornecedor
            AND py.valor IS NOT NULL
            AND px.id_avoid = 0
            AND py.id_avoid = 0
        GROUP BY px.id_fornecedor,
            py.id_fornecedor,
            px.id_base
        HAVING COUNT(px.id_lote) >={{ var('conluio_telefone_num_lotes_juntas_min') }}
            AND (
                (
                    SUM(px.proposta_ganhadora::INTEGER)::DOUBLE PRECISION / NULLIF(COUNT(px.id_lote), 0)
                ) >{{ var('conluio_telefone_taxa_vitoria_no_conluio_min') }}
                OR (
                    SUM(py.proposta_ganhadora::INTEGER)::DOUBLE PRECISION / NULLIF(COUNT(px.id_lote), 0)
                ) >{{ var('conluio_telefone_taxa_vitoria_no_conluio_min') }}
            )
    ) pp
    JOIN same_telephone st ON pp.id_fornecedor_x = st.id_fornecedor_x
    AND pp.id_fornecedor_y = st.id_fornecedor_y