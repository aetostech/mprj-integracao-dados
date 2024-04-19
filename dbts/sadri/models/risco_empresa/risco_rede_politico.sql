WITH empresas_rede AS (
    SELECT t.cnpj,
        t.cpf_politico,
        STRING_AGG(t.conexao, '; '::TEXT) AS conexoes
    FROM (
            SELECT politico.cnpj,
                politico.cpf AS cpf_politico,
                'socio'::TEXT AS conexao
            FROM {{ ref('politico') }}
                JOIN {{ source('unibase', 'fornecedor') }} ON politico.cnpj::TEXT = fornecedor.documento::TEXT
            UNION
            SELECT doadora_campanha.cnpj,
                doadora_campanha.cpf_candidato AS cpf_politico,
                'doador_empresa'::TEXT AS conexao
            FROM {{ ref('doadora_campanha') }}
                JOIN {{ source('unibase', 'fornecedor') }} ON doadora_campanha.cnpj::TEXT = fornecedor.documento::TEXT
            GROUP BY doadora_campanha.cnpj,
                doadora_campanha.cpf_candidato
            HAVING SUM(doadora_campanha.valor_doado) > 20000::DOUBLE PRECISION
            UNION
            SELECT fornecedora_campanha.cnpj,
                fornecedora_campanha.cpf_candidato AS cpf_politico,
                'fornecedor_empresa'::TEXT AS conexao
            FROM {{ ref('fornecedora_campanha') }}
                JOIN {{ source('unibase', 'fornecedor') }} ON fornecedora_campanha.cnpj::TEXT = fornecedor.documento::TEXT
            GROUP BY fornecedora_campanha.cnpj,
                fornecedora_campanha.cpf_candidato
            HAVING SUM(fornecedora_campanha.valor_despesa) > 20000::DOUBLE PRECISION
            UNION
            SELECT doador_campanha.cnpj,
                doador_campanha.cpf_candidato AS cpf_politico,
                'doador_socio'::TEXT AS conexao
            FROM {{ ref('doador_campanha') }}
                JOIN {{ source('unibase', 'fornecedor') }} ON doador_campanha.cnpj::TEXT = fornecedor.documento::TEXT
            GROUP BY doador_campanha.cnpj,
                doador_campanha.cpf_candidato
            HAVING SUM(doador_campanha.valor_doado) > 20000::DOUBLE PRECISION
            UNION
            SELECT fornecedor_campanha.cnpj,
                fornecedor_campanha.cpf_candidato AS cpf_politico,
                'fornecedor_socio'::TEXT AS conexao
            FROM {{ ref('fornecedor_campanha') }}
                JOIN {{ source('unibase', 'fornecedor') }} ON fornecedor_campanha.cnpj::TEXT = fornecedor.documento::TEXT
            GROUP BY fornecedor_campanha.cnpj,
                fornecedor_campanha.cpf_candidato
            HAVING SUM(fornecedor_campanha.valor_despesa) > 20000::DOUBLE PRECISION
        ) t
    GROUP BY t.cnpj,
        t.cpf_politico
),
empresas_risco AS (
    SELECT t1.id_base,
        t1.cnpj,
        STRING_AGG(t1.risco, '; '::TEXT) AS riscos
    FROM (
            SELECT DISTINCT base.id_base,
                beneficiario.cnpj,
                'socio_beneficiario'::TEXT AS risco
            FROM {{ ref('beneficiario') }}
                CROSS JOIN {{ source('unibase', 'base') }}
            UNION
            SELECT DISTINCT 1 AS id_base,
                top_loser.cnpj_empresa_top_loser AS cnpj,
                'top_loser'::TEXT AS risco
            FROM conluio.top_loser
            UNION
            SELECT DISTINCT 1 AS id_base,
                top_loser.cnpj_empresa_vencedora AS cnpj,
                'ganhadora_top_loser'::TEXT AS risco
            FROM conluio.top_loser
            UNION
            SELECT DISTINCT fornecedor.id_base,
                fornecedor.cnpj,
                'precocidade_contrato'::TEXT AS risco
            FROM {{ ref('fornecedor') }}
            WHERE fornecedor.precocidade_contrato_em_dias <= 150
            UNION
            SELECT DISTINCT fornecedor.id_base,
                fornecedor.cnpj,
                'sem_funcionario'::TEXT AS risco
            FROM {{ ref('fornecedor') }}
            WHERE GREATEST(
                    fornecedor.n_funcionarios_2010,
                    fornecedor.n_funcionarios_2011,
                    fornecedor.n_funcionarios_2012,
                    fornecedor.n_funcionarios_2013,
                    fornecedor.n_funcionarios_2014,
                    fornecedor.n_funcionarios_2015,
                    fornecedor.n_funcionarios_2016,
                    fornecedor.n_funcionarios_2017,
                    fornecedor.n_funcionarios_2018
                ) = 0::DOUBLE PRECISION
        ) t1
    GROUP BY t1.cnpj,
        t1.id_base
)
SELECT empresas_risco.id_base,
    empresas_rede.cpf_politico,
    empresas_rede.cnpj,
    empresas_risco.riscos,
    empresas_rede.conexoes
FROM empresas_rede
    JOIN empresas_risco ON empresas_rede.cnpj::TEXT = empresas_risco.cnpj::TEXT