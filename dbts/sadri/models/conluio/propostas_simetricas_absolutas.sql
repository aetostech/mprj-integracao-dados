WITH sim AS (
    SELECT p.id_base,
        p.id_empresa_x,
        p.id_empresa_y,
        p.diferenca_simetria,
        COUNT(DISTINCT p.id_licitacao) AS n_licitacoes_mesma_simetria,
        COUNT(DISTINCT p.id_lote) AS n_lotes_mesma_simetria,
        COALESCE(
            COUNT(
                CASE
                    WHEN p.proposta_ganhadora_x THEN 1
                    ELSE NULL::INTEGER
                END
            ),
            0::BIGINT
        ) AS n_lotes_x_ganhou_simetria,
        COALESCE(
            COUNT(
                CASE
                    WHEN p.proposta_ganhadora_y THEN 1
                    ELSE NULL::INTEGER
                END
            ),
            0::BIGINT
        ) AS n_lotes_y_ganhou_simetria,
        COALESCE(
            SUM(
                CASE
                    WHEN p.proposta_ganhadora_x THEN p.valor_x
                    ELSE NULL::DOUBLE PRECISION
                END
            ),
            0::DOUBLE PRECISION
        ) AS valor_total_x_ganhou_simetria,
        COALESCE(
            SUM(
                CASE
                    WHEN p.proposta_ganhadora_y THEN p.valor_y
                    ELSE NULL::DOUBLE PRECISION
                END
            ),
            0::DOUBLE PRECISION
        ) AS valor_total_y_ganhou_simetria
    FROM (
            WITH propostas_nao_competitivas AS (
                SELECT p_1.id_proposta,
                    p_1.quantidade,
                    p_1.valor_unitario,
                    p_1.valor,
                    p_1.unidade,
                    p_1.data,
                    p_1.proposta_ganhadora,
                    p_1.id_lote,
                    p_1.id_fornecedor,
                    p_1.id_base,
                    p_1.id_avoid
                FROM {{ source('unibase', 'proposta') }} p_1
                    JOIN {{ source('unibase', 'lote') }} l ON p_1.id_lote = l.id_lote
                WHERE l.num_propostas >= 2
                    AND l.num_propostas <={{ var('propostas_simetricas_absolutas_num_propostas_nao_competitivo_max') }}
            ),
            valid_companies AS (
                SELECT propostas_nao_competitivas.id_fornecedor
                FROM propostas_nao_competitivas
                GROUP BY propostas_nao_competitivas.id_fornecedor
                HAVING COUNT(*) >= 2
            )
            SELECT lote.id_licitacao,
                lote.id_lote,
                lote.id_base,
                lote.num_propostas,
                p1.id_fornecedor AS id_empresa_x,
                p1.valor AS valor_x,
                p1.proposta_ganhadora AS proposta_ganhadora_x,
                p2.id_fornecedor AS id_empresa_y,
                p2.valor AS valor_y,
                p2.proposta_ganhadora AS proposta_ganhadora_y,
                ROUND((p1.valor - p2.valor)::NUMERIC, 0) AS diferenca_simetria,
                ROUND((p1.valor / p2.valor)::NUMERIC, 1) AS proporcao_simetria
            FROM propostas_nao_competitivas p1
                JOIN propostas_nao_competitivas p2 ON p1.id_lote = p2.id_lote
                JOIN {{ source('unibase', 'lote') }} ON p1.id_lote = lote.id_lote
            WHERE (
                    p2.id_fornecedor IN (
                        SELECT valid_companies.id_fornecedor
                        FROM valid_companies
                    )
                )
                AND (
                    p1.id_fornecedor IN (
                        SELECT valid_companies.id_fornecedor
                        FROM valid_companies
                    )
                )
                AND p1.id_fornecedor <> p2.id_fornecedor
                AND p1.id_avoid = 0
                AND p2.id_avoid = 0
        ) p
    GROUP BY p.id_empresa_x,
        p.id_empresa_y,
        p.diferenca_simetria,
        p.id_base
    HAVING COUNT(DISTINCT p.id_licitacao)::INTEGER >={{ var('propostas_simetricas_absolutas_num_licitacoes_min') }}
        AND COUNT(DISTINCT p.id_lote) >={{ var('propostas_simetricas_absolutas_num_lotes_min') }}
)
SELECT sim.id_base,
    fx.documento AS cnpj_empresa_x,
    fx.nome AS nome_empresa_x,
    fy.documento AS cnpj_empresa_y,
    fy.nome AS nome_empresa_y,
    sim.diferenca_simetria AS diferenca_valor_xy,
    sim.n_licitacoes_mesma_simetria,
    sim.n_lotes_mesma_simetria,
    sim.n_lotes_x_ganhou_simetria,
    sim.n_lotes_y_ganhou_simetria,
    sim.valor_total_x_ganhou_simetria,
    sim.valor_total_y_ganhou_simetria
FROM sim
    JOIN {{ source('unibase', 'fornecedor') }} fx ON sim.id_empresa_x = fx.id_fornecedor
    JOIN {{ source('unibase', 'fornecedor') }} fy ON sim.id_empresa_y = fy.id_fornecedor
WHERE sim.valor_total_x_ganhou_simetria >={{ var('propostas_simetricas_absolutas_valor_ganho_no_conluio_min') }}
    OR sim.valor_total_y_ganhou_simetria >={{ var('propostas_simetricas_absolutas_valor_ganho_no_conluio_min') }};