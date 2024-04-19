SELECT
    socio.cnpj,
    socio.documento_socio AS cpf_socio_beneficiario,
    beneficios_sociais.nis_beneficiario AS nis_socio_beneficiario,
    socio.data_entrada_sociedade,
    socio.data_saida_sociedade,
    socio.nome_socio,
    string_agg(
        beneficios_sociais.id_tipo_beneficio :: text,
        '; ' :: text
    ) AS beneficio_social,
    SUM(
        CASE
            WHEN beneficios_sociais.id_tipo_beneficio :: text = 'bpc' :: text THEN 1
            ELSE 0
        END
    ) AS socio_beneficiario_bpc,
    SUM(
        CASE
            WHEN beneficios_sociais.id_tipo_beneficio :: text = 'bolsa_familia' :: text THEN 1
            ELSE 0
        END
    ) AS socio_beneficiario_bolsa_familia,
    SUM(
        CASE
            WHEN beneficios_sociais.id_tipo_beneficio :: text = 'garantia_safra' :: text THEN 1
            ELSE 0
        END
    ) AS socio_beneficiario_garantia_safra,
    SUM(
        CASE
            WHEN beneficios_sociais.id_tipo_beneficio :: text = 'peti' :: text THEN 1
            ELSE 0
        END
    ) AS socio_beneficiario_peti,
    SUM(
        CASE
            WHEN beneficios_sociais.id_tipo_beneficio :: text = 'seguro_defeso' :: text THEN 1
            ELSE 0
        END
    ) AS socio_beneficiario_seguro_defeso
FROM
    {{ source('dados_cadastrais', 'beneficios_sociais') }} beneficios_sociais
    JOIN (
        SELECT
            DISTINCT socio_1.cnpj,
            socio_1.documento_socio,
            socio_1.nome_socio,
            socio_1.data_entrada_sociedade,
            NULL :: text AS data_saida_sociedade
        FROM
            {{ source('dados_cadastrais', 'socio') }} socio_1
        WHERE
            LENGTH(
                socio_1.documento_socio :: text
            ) = 11
    ) socio
    ON "substring"(
        beneficios_sociais.cpf_beneficiario_anonimizado :: text,
        4,
        6
    ) = "substring"(
        socio.documento_socio :: text,
        4,
        6
    )
    AND beneficios_sociais.nome_beneficiario :: text = socio.nome_socio :: text
GROUP BY
    socio.cnpj,
    socio.documento_socio,
    beneficios_sociais.nis_beneficiario,
    socio.data_entrada_sociedade,
    socio.data_saida_sociedade,
    socio.nome_socio
