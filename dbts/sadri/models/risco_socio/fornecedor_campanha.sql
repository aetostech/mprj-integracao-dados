SELECT
    DISTINCT pjs.cnpj,
    tdc.documento_fornecedor AS cpf,
    tdc.valor_despesa,
    tdc.cpf_candidato,
    n.nome_candidato,
    tdc.ano,
    tdc.partido,
    tdc.cargo,
    tdc.municipio,
    tdc.uf,
    tdc.eleito
FROM
    {{ source(
        'dados_cadastrais',
        'despesa_campanha'
    ) }}
    tdc
    JOIN (
        SELECT
            socio.cnpj,
            socio.documento_socio
        FROM
            {{ source(
                'dados_cadastrais',
                'socio'
            ) }}
    ) pjs
    ON tdc.documento_fornecedor :: text = pjs.documento_socio :: text
    JOIN (
        SELECT
            candidatura.cpf,
            MAX(
                candidatura.nome :: text
            ) AS nome_candidato
        FROM
            {{ source(
                'dados_cadastrais',
                'candidatura'
            ) }}
        GROUP BY
            candidatura.cpf
    ) n
    ON tdc.cpf_candidato :: bpchar = n.cpf
