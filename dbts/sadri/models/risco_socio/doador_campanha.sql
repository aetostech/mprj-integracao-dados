SELECT DISTINCT socio.cnpj,
    socio.documento_socio AS cpf,
    doacao_campanha.valor_doacao AS valor_doado,
    doacao_campanha.cpf_candidato,
    doacao_campanha.nome_candidato,
    doacao_campanha.ano,
    doacao_campanha.partido,
    doacao_campanha.cargo,
    doacao_campanha.municipio,
    doacao_campanha.uf,
    doacao_campanha.eleito
FROM {{ source('dados_cadastrais', 'doacao_campanha') }} doacao_campanha
    JOIN (
        SELECT socio_1.cnpj,
            socio_1.documento_socio,
            socio_1.nome_socio
        FROM {{ source('dados_cadastrais', 'socio') }} socio_1
    ) socio ON "substring"(socio.documento_socio::TEXT, 4, 6) = "substring"(doacao_campanha.documento_doador::TEXT, 4, 6)
    AND socio.nome_socio = doacao_campanha.nome_doador
WHERE LENGTH(socio.documento_socio::TEXT) = 11