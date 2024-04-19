SELECT punicao.documento AS cnpj,
    punicao.base_origem,
    punicao.tipo_punicao,
    punicao.data_coleta,
    punicao.orgao_punicao
FROM {{ source('dados_cadastrais', 'punicao') }}
WHERE punicao.tipo_documento::TEXT = 'J'::TEXT