SELECT ul.id_lote,
    ul.num_propostas,
    ul.quantidade AS quantidade_lote,
    ul.id_entidade,
    ent.id_orgao_superior,
    ul.valor_estimado,
    ml.proposta_unit_max,
    ml.proposta_unit_min,
    ml.proposta_total_max,
    ml.proposta_total_min,
    ml.gap_relativo_maior_menor_proposta,
    ml.gap_relativo_maior_proposta_e_media,
    ml.gap_relativo_menor_proposta_e_media,
    ml.n_doadoras_campanha_politica,
    ml.n_fornecedoras_campanha_politica,
    ml.n_empresas_punidas,
    ml.empresa_vencedora_punida,
    ml.n_empresas_punidas::DOUBLE PRECISION / ul.num_propostas::DOUBLE PRECISION AS proporcao_empresas_punidas,
    ml.n_empresas_cnae_guarda_chuva,
    ml.n_empresas_socios_beneficiarios_caso_fraco,
    ml.n_empresas_socios_beneficiarios_caso_fraco::DOUBLE PRECISION / ul.num_propostas::DOUBLE PRECISION AS proporcao_empresas_socio_beneficiario_caso_fraco,
    ml.n_empresas_socio_politico,
    ml.n_empresas_socio_politico::DOUBLE PRECISION / ul.num_propostas::DOUBLE PRECISION AS proporcao_empresas_socio_politico,
    ml.n_empresas_precocidade_proposta_ate_90,
    ml.n_empresas_precocidade_proposta_ate_90::DOUBLE PRECISION / ul.num_propostas::DOUBLE PRECISION AS proporcao_empresas_precocidade_proposta_ate_90,
    ml.n_empresas_precocidade_contrato_ate_90,
    ml.n_empresas_precocidade_contrato_ate_90::DOUBLE PRECISION / ul.num_propostas::DOUBLE PRECISION AS proporcao_empresas_precocidade_contrato_ate_90,
    ml.n_empresas_top_losers,
    ml.n_empresas_top_losers::DOUBLE PRECISION / ul.num_propostas::DOUBLE PRECISION AS proporcao_empresas_top_losers,
    ml.empresa_que_venceu_ganhadora_top_loser,
    ml.valor_proposta_ganhadora,
    ul.data,
    ul.id_base
FROM {{ source('unibase', 'lote') }} ul
    JOIN (
        SELECT up.id_lote,
            MAX(up.valor_unitario) AS proposta_unit_max,
            MIN(up.valor_unitario) AS proposta_unit_min,
            MAX(up.valor) AS proposta_total_max,
            MIN(up.valor) AS proposta_total_min,
            CASE
                WHEN MIN(up.valor_unitario) <> 0::DOUBLE PRECISION THEN (MAX(up.valor_unitario) - MIN(up.valor_unitario)) / MIN(up.valor_unitario)
                ELSE NULL::DOUBLE PRECISION
            END AS gap_relativo_maior_menor_proposta,
            CASE
                WHEN AVG(up.valor_unitario) <> 0::DOUBLE PRECISION THEN (MAX(up.valor_unitario) - AVG(up.valor_unitario)) / AVG(up.valor_unitario)
                ELSE NULL::DOUBLE PRECISION
            END AS gap_relativo_maior_proposta_e_media,
            CASE
                WHEN AVG(up.valor_unitario) <> 0::DOUBLE PRECISION THEN (AVG(up.valor_unitario) - MIN(up.valor_unitario)) / AVG(up.valor_unitario)
                ELSE NULL::DOUBLE PRECISION
            END AS gap_relativo_menor_proposta_e_media,
            COALESCE(
                COUNT(
                    CASE
                        WHEN mr.doadora_campanha_politica THEN 1
                        ELSE NULL::INTEGER
                    END
                ),
                0::BIGINT
            ) AS n_doadoras_campanha_politica,
            COALESCE(
                COUNT(
                    CASE
                        WHEN mr.fornecedora_campanha_politica THEN 1
                        ELSE NULL::INTEGER
                    END
                ),
                0::BIGINT
            ) AS n_fornecedoras_campanha_politica,
            COALESCE(
                COUNT(
                    CASE
                        WHEN mr.punida_ceis THEN 1
                        ELSE NULL::INTEGER
                    END
                ),
                0::BIGINT
            ) AS n_punidas_ceis,
            COALESCE(
                COUNT(
                    CASE
                        WHEN mr.punida_cepim THEN 1
                        ELSE NULL::INTEGER
                    END
                ),
                0::BIGINT
            ) AS n_punidas_cepim,
            COALESCE(
                COUNT(
                    CASE
                        WHEN mr.punida_cnep THEN 1
                        ELSE NULL::INTEGER
                    END
                ),
                0::BIGINT
            ) AS n_punidas_cnep,
            COALESCE(
                COUNT(
                    CASE
                        WHEN mr.punida_al THEN 1
                        ELSE NULL::INTEGER
                    END
                ),
                0::BIGINT
            ) AS n_punidas_al,
            COALESCE(
                COUNT(
                    CASE
                        WHEN mr.punida_tce_sp THEN 1
                        ELSE NULL::INTEGER
                    END
                ),
                0::BIGINT
            ) AS n_punidas_tce_sp,
            COALESCE(
                COUNT(
                    CASE
                        WHEN mr.punida_lte THEN 1
                        ELSE NULL::INTEGER
                    END
                ),
                0::BIGINT
            ) AS n_punidas_lte,
            COALESCE(
                SUM(
                    (
                        mr.punida_ceis
                        OR mr.punida_cepim
                        OR mr.punida_cnep
                        OR mr.punida_al
                        OR mr.punida_tce_sp
                        OR mr.punida_lte
                    )::INTEGER
                ),
                0::BIGINT
            ) AS n_empresas_punidas,
            BOOL_OR(
                up.proposta_ganhadora
                AND (
                    mr.punida_ceis
                    OR mr.punida_cepim
                    OR mr.punida_cnep
                    OR mr.punida_al
                    OR mr.punida_tce_sp
                    OR mr.punida_lte
                )
            ) AS empresa_vencedora_punida,
            COALESCE(
                SUM(
                    (
                        mr.n_cnaes >= 10
                        AND mr.n_cnaes_divisoes >= 5
                    )::INTEGER
                ),
                0::BIGINT
            ) AS n_empresas_cnae_guarda_chuva,
            COALESCE(
                COUNT(
                    CASE
                        WHEN mr.socio_beneficiario_caso_fraco THEN 1
                        ELSE NULL::INTEGER
                    END
                ),
                0::BIGINT
            ) AS n_empresas_socios_beneficiarios_caso_fraco,
            COALESCE(
                COUNT(
                    CASE
                        WHEN mr.socio_politico THEN 1
                        ELSE NULL::INTEGER
                    END
                ),
                0::BIGINT
            ) AS n_empresas_socio_politico,
            COALESCE(
                COUNT(
                    CASE
                        WHEN mr.precocidade_proposta_em_dias <= 90 THEN 1
                        ELSE NULL::INTEGER
                    END
                ),
                0::BIGINT
            ) AS n_empresas_precocidade_proposta_ate_90,
            COALESCE(
                COUNT(
                    CASE
                        WHEN mr.precocidade_contrato_em_dias <= 90 THEN 1
                        ELSE NULL::INTEGER
                    END
                ),
                0::BIGINT
            ) AS n_empresas_precocidade_contrato_ate_90,
            COALESCE(
                COUNT(
                    CASE
                        WHEN mr.empresa_top_loser THEN 1
                        ELSE NULL::INTEGER
                    END
                ),
                0::BIGINT
            ) AS n_empresas_top_losers,
            BOOL_OR(
                up.proposta_ganhadora
                AND mr.empresa_ganhadora_top_loser
            ) AS empresa_que_venceu_ganhadora_top_loser,
            SUM(
                CASE
                    WHEN up.proposta_ganhadora THEN up.valor
                    ELSE 0::DOUBLE PRECISION
                END
            ) AS valor_proposta_ganhadora
        FROM {{ source('unibase', 'proposta') }} up
            JOIN {{ source('unibase', 'fornecedor') }} f ON up.id_fornecedor = f.id_fornecedor
            JOIN {{ ref('fornecedor') }} mr ON up.id_fornecedor = mr.id_fornecedor
            AND up.id_base = mr.id_base
            AND up.id_avoid = 0
        GROUP BY up.id_lote
    ) ml ON ul.id_lote = ml.id_lote
    LEFT JOIN {{ source('unibase', 'entidade') }} ent ON ul.id_entidade = ent.id_entidade
    AND ul.num_propostas <> 0