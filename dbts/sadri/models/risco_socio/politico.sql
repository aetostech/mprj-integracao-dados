SELECT
    DISTINCT pjs.cnpj,
    tc.cpf,
    tc.ano,
    tc.partido,
    tc.cargo,
    tc.municipio,
    tc.uf,
    tc.eleito
FROM
    {{ source(
        'dados_cadastrais',
        'candidatura'
    ) }}
    tc
    JOIN (
        SELECT
            socio.cnpj,
            socio.documento_socio,
            socio.nome_socio
        FROM
            {{ source(
                'dados_cadastrais',
                'socio'
            ) }}
    ) pjs
    ON "substring"(
        tc.cpf,
        4,
        6
    ) = "substring"(
        pjs.documento_socio :: bpchar,
        4,
        6
    )
    AND tc.nome = pjs.nome_socio
