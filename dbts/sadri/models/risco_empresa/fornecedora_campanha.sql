SELECT tdc.documento_fornecedor AS cnpj,
    tc.nome_candidato,
    tdc.cpf_candidato,
    tdc.ano,
    tdc.partido,
    tdc.cargo,
    tdc.municipio,
    tdc.uf,
    tdc.eleito,
    tdc.valor_despesa
FROM {{ source('dados_cadastrais', 'despesa_campanha') }} tdc
    LEFT JOIN (
        SELECT candidatura.cpf AS cpf_candidato,
            MAX(candidatura.nome::TEXT) AS nome_candidato
        FROM {{ source('dados_cadastrais', 'candidatura') }} candidatura
        GROUP BY candidatura.cpf
    ) tc ON tdc.cpf_candidato::bpchar = tc.cpf_candidato