SELECT
    DISTINCT socio.cnpj,
    socio.documento_socio AS cpf,
    punicao.base_origem,
    punicao.tipo_punicao,
    punicao.data_coleta,
    punicao.orgao_punicao
FROM
    {{ source(
        'dados_cadastrais',
        'punicao'
    ) }}
    punicao
    JOIN (
        SELECT
            socio_1.cnpj,
            socio_1.documento_socio,
            socio_1.nome_socio
        FROM
            {{ source(
                'dados_cadastrais',
                'socio'
            ) }}
            socio_1
    ) socio
    ON "substring"(
        punicao.documento :: text,
        4,
        6
    ) = "substring"(
        socio.documento_socio :: text,
        4,
        6
    )
    AND punicao.nome = socio.nome_socio
WHERE
    punicao.tipo_documento :: text = 'F' :: text
