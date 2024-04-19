SELECT doacao_campanha.documento_doador AS cnpj,
    doacao_campanha.nome_candidato,
    doacao_campanha.cpf_candidato,
    doacao_campanha.ano,
    doacao_campanha.partido,
    doacao_campanha.cargo,
    doacao_campanha.municipio,
    doacao_campanha.uf,
    doacao_campanha.eleito,
    doacao_campanha.valor_doacao AS valor_doado
FROM {{ source('dados_cadastrais', 'doacao_campanha') }} doacao_campanha