WITH valid_lot AS (
    SELECT DISTINCT l.id_lote
    FROM {{ source('unibase', 'lote') }} l
    WHERE (
            l.num_propostas >= 2
            AND l.num_propostas <={{ var('conluio_top_loser_num_propostas_nao_competitivo_max') }}
        )
        AND l.id_base = 1
),
not_top_loser_contracts AS (
    SELECT p.id_fornecedor
    FROM (
            SELECT contrato.id_fornecedor,
                SUM(contrato.valor) AS valor_total_contratos
            FROM {{ source('unibase', 'contrato') }}
            WHERE contrato.id_avoid = 0
                AND contrato.id_base = 1
            GROUP BY contrato.id_fornecedor
        ) c
        JOIN (
            SELECT proposta.id_fornecedor,
                SUM(proposta.valor) AS valor_total_propostas
            FROM {{ source('unibase', 'proposta') }}
            WHERE proposta.id_avoid = 0
                AND proposta.id_base = 1
            GROUP BY proposta.id_fornecedor
        ) p ON c.id_fornecedor = p.id_fornecedor
        JOIN {{ source('unibase', 'fornecedor') }} f ON f.id_fornecedor = c.id_fornecedor
    WHERE c.valor_total_contratos > (
           {{ var('conluio_top_loser_proporcao_contrato_proposta_max') }} * p.valor_total_propostas
        )
        OR c.valor_total_contratos >{{ var('conluio_top_loser_valor_contratos_max') }}
    GROUP BY p.id_fornecedor
),
proposals_grouped AS (
    SELECT DATE_PART('year'::TEXT, l.data) AS ano,
        p.id_fornecedor,
        SUM(p.valor) AS valor_total_propostas_lotes_validos,
        SUM(
            CASE
                WHEN p.proposta_ganhadora THEN p.valor
                ELSE 0::DOUBLE PRECISION
            END
        ) AS valor_total_ganho_lotes_validos,
        SUM(
            CASE
                WHEN p.proposta_ganhadora THEN 1
                ELSE 0
            END
        ) AS n_vitorias_lotes_validos,
        COUNT(DISTINCT p.id_lote) AS n_lotes_validos,
        AVG(l.num_propostas) AS n_medio_licitantes
    FROM {{ source('unibase', 'proposta') }} p
        JOIN {{ source('unibase', 'lote') }} l ON p.id_lote = l.id_lote
        AND p.id_base = l.id_base
        JOIN valid_lot vl ON l.id_lote = vl.id_lote
    WHERE p.id_avoid = 0
        AND p.id_base = 1
    GROUP BY p.id_fornecedor,
        (DATE_PART('year'::TEXT, l.data))
),
top_loser_companies AS (
    SELECT DATE_PART('year'::TEXT, l.data) AS ano,
        p.id_fornecedor,
        SUM(
            CASE
                WHEN p.proposta_ganhadora THEN 1
                ELSE 0
            END
        ) AS n_vitorias_lotes_validos,
        SUM(
            CASE
                WHEN p.proposta_ganhadora THEN p.valor
                ELSE 0::DOUBLE PRECISION
            END
        ) AS valor_ganho_lotes_validos,
        COUNT(DISTINCT p.id_lote) AS n_propostas_lotes_validos
    FROM {{ source('unibase', 'proposta') }} p
        JOIN valid_lot vl ON p.id_lote = vl.id_lote
        JOIN {{ source('unibase', 'lote') }} l ON l.id_lote = vl.id_lote
    WHERE p.id_avoid = 0
        AND l.id_base = 1
        AND NOT (
            p.id_fornecedor IN (
                SELECT not_top_loser_contracts.id_fornecedor
                FROM not_top_loser_contracts
            )
        )
    GROUP BY p.id_fornecedor,
        (DATE_PART('year'::TEXT, l.data))
    HAVING COUNT(DISTINCT p.id_lote) >={{ var('conluio_top_loser_num_lotes_conluio_min') }}
        AND SUM(
            CASE
                WHEN p.proposta_ganhadora THEN p.valor
                ELSE 0::DOUBLE PRECISION
            END
        ) <{{ var('conluio_top_loser_valor_proposta_ganhadora_max') }}
        AND SUM(
            CASE
                WHEN p.proposta_ganhadora THEN 1
                ELSE 0
            END
        )::NUMERIC <= (
           {{ var('conluio_top_loser_taxa_vitoria_max') }} * COUNT(DISTINCT p.id_lote)::NUMERIC
        )
),
top_loser_lot AS (
    SELECT DISTINCT p.id_lote
    FROM top_loser_companies tlc
        JOIN {{ source('unibase', 'proposta') }} p ON p.id_fornecedor = tlc.id_fornecedor
        JOIN valid_lot vl ON p.id_lote = vl.id_lote
    WHERE p.id_avoid = 0
        AND p.id_base = 1
),
top_winners AS (
    SELECT pg.ano,
        p.id_fornecedor,
        COUNT(DISTINCT p.id_lote) AS n_vitorias_sobre_top_loser,
        SUM(p.valor) AS valor_total_vencido_sobre_top_loser
    FROM top_loser_lot tll
        JOIN {{ source('unibase', 'proposta') }} p ON p.id_lote = tll.id_lote
        JOIN proposals_grouped pg ON p.id_fornecedor = pg.id_fornecedor
    WHERE p.proposta_ganhadora
        AND p.id_base = 1
        AND p.id_avoid = 0
        AND NOT (
            p.id_fornecedor IN (
                SELECT top_loser_companies.id_fornecedor
                FROM top_loser_companies
            )
        )
    GROUP BY p.id_fornecedor,
        pg.ano
    HAVING COUNT(DISTINCT p.id_lote)::NUMERIC > (
            AVG(pg.n_vitorias_lotes_validos) *{{ var('conluio_top_loser_taxa_vitoria_sobre_top_loser_min') }}
        )
        AND SUM(p.valor) > (
            AVG(pg.valor_total_ganho_lotes_validos) *{{ var('conluio_top_loser_significancia_sobre_top_loser_min') }}
        )
),
pair_proposals_winner_loser AS (
    SELECT pgx_1.ano,
        px.id_fornecedor AS id_empresa_vencedora,
        py.id_fornecedor AS id_empresa_top_loser,
        COUNT(DISTINCT px.id_lote) AS n_lotes_nao_competitivos_juntas,
        SUM(px.proposta_ganhadora::INTEGER) AS n_vitorias_sobre_esta_top_loser_nao_competitivo,
        SUM(
            CASE
                WHEN px.proposta_ganhadora THEN px.valor
                ELSE 0::DOUBLE PRECISION
            END
        ) AS valor_total_vencido_sobre_esta_top_loser_nao_competitivo,
        SUM(px.valor) AS valor_proposto_winner,
        AVG(pgx_1.n_lotes_validos) AS n_lotes_nao_competitivos_vencedora,
        AVG(pgy_1.n_lotes_validos) AS n_lotes_nao_competitivos_loser
    FROM top_loser_companies tlc
        JOIN {{ source('unibase', 'proposta') }} py ON py.id_fornecedor = tlc.id_fornecedor
        JOIN {{ source('unibase', 'proposta') }} px ON px.id_lote = py.id_lote
        JOIN {{ source('unibase', 'lote') }} l ON px.id_lote = l.id_lote
        JOIN proposals_grouped pgx_1 ON px.id_fornecedor = pgx_1.id_fornecedor
        JOIN proposals_grouped pgy_1 ON py.id_fornecedor = pgy_1.id_fornecedor
        JOIN top_winners tw ON px.id_fornecedor = tw.id_fornecedor
        JOIN valid_lot vl ON px.id_lote = vl.id_lote
    WHERE px.id_avoid = 0
        AND pgx_1.ano = pgy_1.ano
        AND pgx_1.ano = DATE_PART('year'::TEXT, l.data)
        AND pgx_1.ano = tw.ano
        AND pgx_1.ano = tlc.ano
        AND py.id_avoid = 0
        AND px.id_base = 1
        AND py.id_base = 1
    GROUP BY pgx_1.ano,
        px.id_fornecedor,
        py.id_fornecedor
    HAVING SUM(px.valor) <> 0::DOUBLE PRECISION
        AND COUNT(DISTINCT px.id_lote) >={{ var('conluio_top_loser_num_lotes_conluio_min') }}
        AND SUM(px.proposta_ganhadora::INTEGER) >={{ var('conluio_top_loser_num_vitorias_conluio_min') }}
        AND SUM(
            CASE
                WHEN px.proposta_ganhadora THEN px.valor
                ELSE 0::DOUBLE PRECISION
            END
        ) > 0::DOUBLE PRECISION
),
mean_competitors_winner_loser AS (
    SELECT ppwlx.ano,
        px.id_fornecedor AS id_empresa_vencedora,
        py.id_fornecedor AS id_empresa_top_loser,
        AVG(l.num_propostas) AS n_medio_licitantes_juntas_todos_lotes
    FROM pair_proposals_winner_loser ppwlx
        JOIN {{ source('unibase', 'proposta') }} px ON px.id_fornecedor = ppwlx.id_empresa_vencedora
        JOIN {{ source('unibase', 'proposta') }} py ON px.id_lote = py.id_lote
        JOIN pair_proposals_winner_loser ppwly ON py.id_fornecedor = ppwly.id_empresa_top_loser
        JOIN {{ source('unibase', 'lote') }} l ON l.id_lote = px.id_lote
    WHERE px.id_avoid = 0
        AND ppwlx.ano = DATE_PART('year'::TEXT, l.data)
        AND ppwlx.ano = ppwly.ano
        AND py.id_avoid = 0
        AND px.id_base = 1
        AND py.id_base = 1
    GROUP BY ppwlx.ano,
        px.id_fornecedor,
        py.id_fornecedor
)
SELECT 1 AS id_base,
    ppwl.ano,
    fx.documento AS cnpj_empresa_vencedora,
    fy.documento AS cnpj_empresa_top_loser,
    fx.nome AS nome_empresa_vencedora,
    fy.nome AS nome_empresa_top_loser,
    ppwl.n_lotes_nao_competitivos_juntas,
    ppwl.n_vitorias_sobre_esta_top_loser_nao_competitivo,
    ppwl.valor_total_vencido_sobre_esta_top_loser_nao_competitivo,
    ppwl.n_lotes_nao_competitivos_vencedora,
    ppwl.n_lotes_nao_competitivos_loser,
    mcwl.n_medio_licitantes_juntas_todos_lotes,
    pgx.n_medio_licitantes AS n_medio_licitantes_vencedora_todos_lotes,
    pgy.n_medio_licitantes AS n_medio_licitantes_loser_todos_lotes
FROM pair_proposals_winner_loser ppwl
    JOIN mean_competitors_winner_loser mcwl ON ppwl.id_empresa_vencedora = mcwl.id_empresa_vencedora
    AND ppwl.ano = mcwl.ano
    AND ppwl.id_empresa_top_loser = mcwl.id_empresa_top_loser
    JOIN {{ source('unibase', 'fornecedor') }} fx ON ppwl.id_empresa_vencedora = fx.id_fornecedor
    JOIN {{ source('unibase', 'fornecedor') }} fy ON ppwl.id_empresa_top_loser = fy.id_fornecedor
    JOIN proposals_grouped pgx ON ppwl.id_empresa_vencedora = pgx.id_fornecedor
    AND ppwl.ano = pgx.ano
    JOIN proposals_grouped pgy ON ppwl.id_empresa_top_loser = pgy.id_fornecedor
    AND ppwl.ano = pgy.ano